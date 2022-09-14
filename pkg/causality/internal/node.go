// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"sync"
	stdAtomic "sync/atomic"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
)

const (
	unassigned    = workerID(-2)
	invalidNodeID = int64(-1)
)

var (
	nextNodeID = atomic.NewInt64(0)
	nodePool   = &sync.Pool{}

	// btreeFreeList is a shared free list used by all
	// btrees in order to lessen the burden of GC.
	//
	// Experiment shows increasing the capacity beyond 1024 yields little
	// performance improvement.
	btreeFreeList = btree.NewFreeListG[*Node](1024)
)

// Node is a node in the dependency graph used
// in conflict detection.
type Node struct {
	// Immutable fields.
	id           int64
	OnResolved   func(id workerID)
	RandWorkerID func() workerID

	totalDependees    int32
	resolvedDependees int32
	removedDependees  int32
	dependees         []int64

	// Following fields are protected by `mu`.
	mu sync.Mutex

	assignedTo workerID
	removed    bool

	// dependers is an ordered set for all nodes that
	// conflict with the current node.
	//
	// Notes:
	// (1) An ordered data structure is preferred because
	//     if we can unblock conflicting transactions in the
	//     order that they have come in, the out-of-order-ness
	//     observed downstream will be less than what would have been
	//     if an unordered set were used.
	// (2) Google's btree package is selected because it seems to be
	//     the most popular production-grade ordered set implementation in Go.
	dependers *btree.BTreeG[*Node]
}

// NewNode creates a new node.
func NewNode() (ret *Node) {
	defer func() {
		ret.id = nextNodeID.Add(1)
		ret.OnResolved = nil
		ret.RandWorkerID = nil
		ret.assignedTo = unassigned
		ret.removed = false
		ret.totalDependees = 0
		ret.resolvedDependees = 0
		ret.removedDependees = 0
	}()

	if obj := nodePool.Get(); obj != nil {
		ret = obj.(*Node)
	} else {
		ret = new(Node)
	}
	return
}

// NodeID implements interface internal.SlotNode.
func (n *Node) NodeID() int64 {
	return n.id
}

// DependOn implements interface internal.SlotNode.
func (n *Node) DependOn(others map[int64]*Node) {
	depend := func(target *Node) {
		if target.id == n.id {
			panic("you cannot depend on yourself")
		}
		// Lock target and insert `n` into target.dependers.
		target.mu.Lock()
		defer target.mu.Unlock()
		if target.assignedTo != unassigned {
			resolvedDependees := stdAtomic.AddInt32(&n.resolvedDependees, 1)
			stdAtomic.StoreInt64(&n.dependees[resolvedDependees-1], target.assignedTo)
		}
		if target.removed {
			stdAtomic.AddInt32(&n.removedDependees, 1)
		} else if _, exist := target.getOrCreateDependers().ReplaceOrInsert(n); exist {
			panic("should never exist")
		}
	}

	n.totalDependees = int32(len(others))
	n.dependees = make([]int64, 0, n.totalDependees)
	for i := 0; i < int(n.totalDependees); i++ {
		n.dependees = append(n.dependees, unassigned)
	}

	for _, target := range others {
		depend(target)
	}

	n.maybeResolve()
}

// Remove implements interface internal.SlotNode.
func (n *Node) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.removed = true
	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			removedDependees := stdAtomic.AddInt32(&node.removedDependees, 1)
			if removedDependees == node.totalDependees {
				node.maybeResolve()
			}
			return true
		})
		n.dependers.Clear(true)
		n.dependers = nil
	}
}

// Free implements interface internal.SlotNode.
// It must be called if a node is no longer used.
// We are using sync.Pool to lessen the burden of GC.
func (n *Node) Free() {
	if n.id == invalidNodeID {
		panic("double free")
	}

	n.id = invalidNodeID
	n.OnResolved = nil
	n.RandWorkerID = nil
	n.totalDependees = 0
	n.resolvedDependees = 0
	n.removedDependees = 0
	n.assignedTo = unassigned
	n.removed = false

	nodePool.Put(n)
}

// assignTo assigns a node to a worker.
func (n *Node) assignTo(workerID int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.assignedTo != unassigned {
		// Already resolved by some other guys.
		return false
	}
	n.assignedTo = workerID
	return true
}

func (n *Node) notifyDependers() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			resolvedDependees := stdAtomic.AddInt32(&node.resolvedDependees, 1)
			stdAtomic.StoreInt64(&node.dependees[resolvedDependees-1], n.assignedTo)
			if resolvedDependees == node.totalDependees {
				node.maybeResolve()
			}
			return true
		})
	}
}

func (n *Node) maybeResolve() {
	if workerNum, ok := n.tryResolve(); ok {
		if workerNum < 0 {
			panic("Node.tryResolve must return a valid worker ID")
		}
		if n.assignTo(workerNum) {
			n.OnResolved(workerNum)
			n.OnResolved = nil
			n.notifyDependers()
		}
	}
	return
}

// tryResolve must be called with n.mu locked.
// Returns (_, false) if there is a conflict,
// returns (rand, true) if there is no conflict,
// returns (N, true) if only worker N can be used.
func (n *Node) tryResolve() (int64, bool) {
	if n.totalDependees == 0 {
		// No conflicts, can select any workers.
		return n.RandWorkerID(), true
	}

	resolvedDependees := stdAtomic.LoadInt32(&n.resolvedDependees)
	if resolvedDependees == n.totalDependees {
		// NOTE: We don't pick the last unremoved worker because lots of tasks can be
		// attached to that worker after a time.
		if n.totalDependees == 1 {
			return stdAtomic.LoadInt64(&n.dependees[0]), true
		} else if n.totalDependees == stdAtomic.LoadInt32(&n.removedDependees) {
			return n.RandWorkerID(), true
		}
	}
	return unassigned, false
}

func (n *Node) getOrCreateDependers() *btree.BTreeG[*Node] {
	if n.dependers == nil {
		n.dependers = btree.NewWithFreeListG(8, func(a, b *Node) bool {
			return a.id < b.id
		}, btreeFreeList)
	}
	return n.dependers
}

// dependerCount returns the number of dependers the node has.
// NOTE: dependerCount is used for unit tests only.
func (n *Node) dependerCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dependers == nil {
		return 0
	}
	return n.dependers.Len()
}

// assignedWorkerID returns the worker ID that the node has been assigned to.
// NOTE: assignedWorkerID is used for unit tests only.
func (n *Node) assignedWorkerID() workerID {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.assignedTo
}
