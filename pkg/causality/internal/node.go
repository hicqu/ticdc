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
	"fmt"
	"sync"
	stdAtomic "sync/atomic"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
)

const (
	unassigned    = workerID(-1)
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
	id         int64
	OnResolved func(id workerID)

	resolved atomic.Bool

	totalDependees    int32
	resolvedDependees int32
	removedDependees  int32
	dependees         []int64

	// Following fields are protected by `mu`.
	mu sync.Mutex

	assignedTo workerID

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
		ret.assignedTo = unassigned
		ret.totalDependees = 0
		ret.resolvedDependees = 0
		ret.removedDependees = 0
	}()

	if obj := nodePool.Get(); obj != nil {
		return obj.(*Node)
	}
	return new(Node)
}

// Equals implements interface internal.SlotNode.
func (n *Node) Equals(other *Node) bool {
	return n.id == other.id
}

// DependOn implements interface internal.SlotNode.
func (n *Node) DependOn(others []*Node) {
	fmt.Printf("Node.DependOn %d nodes\n", len(others))
	depend := func(target *Node) {
		if target.id == n.id {
			panic("you cannot depend on yourself")
		}
		// Lock target and insert `n` into target.dependers.
		target.mu.Lock()
		defer target.mu.Unlock()
		target.getOrCreateDependers().ReplaceOrInsert(n)
		if target.assignedTo != unassigned {
			resolvedDependees := stdAtomic.AddInt32(&n.resolvedDependees, 1)
			n.dependees[resolvedDependees-1] = target.assignedTo
		}
	}

	n.totalDependees += int32(len(others))
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
	n.remove()
	n.free()
}

// assignTo assigns a node to a worker.
func (n *Node) assignTo(workerID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.assignedTo = workerID
	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			resolvedDependees := stdAtomic.AddInt32(&node.resolvedDependees, 1)
			node.dependees[resolvedDependees-1] = workerID
			if resolvedDependees == node.totalDependees {
				node.maybeResolve()
			}
			return true
		})
	}
}

// remove should be called after the transaction corresponding to the node is finished.
func (n *Node) remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			removedDependees := stdAtomic.AddInt32(&node.removedDependees, 1)
			if removedDependees == node.totalDependees {
				node.maybeResolve()
			}
			return true
		})
		n.dependers.Clear(true)
	}
}

// free must be called if a node is no longer used.
// We are using sync.Pool to lessen the burden of GC.
func (n *Node) free() {
	if n.id == invalidNodeID {
		panic("double free")
	}

	n.id = invalidNodeID
	n.assignedTo = unassigned
	n.OnResolved = nil
	n.resolved.Store(false)

	nodePool.Put(n)
}

func (n *Node) maybeResolve() {
	if workerNum, ok := n.tryResolve(); ok && !n.resolved.Swap(true) {
		n.OnResolved(workerNum)
		n.assignTo(workerNum)
	}
	return
}

// tryResolve must be called with n.mu locked.
// Returns (_, false) if there is a conflict,
// returns (-1, true) if there is no conflict,
// returns (N, true) if only worker N can be used.
func (n *Node) tryResolve() (int64, bool) {
	if n.totalDependees == 0 {
		// No conflicts, can select any workers.
		return -1, true
	}

	resolvedDependees := stdAtomic.LoadInt32(&n.resolvedDependees)
	if resolvedDependees == n.totalDependees {
		// NOTE: We don't pick the last unremoved worker because lots of tasks can be
		// attached to that worker after a time.
		if n.totalDependees == 1 || n.totalDependees == stdAtomic.LoadInt32(&n.removedDependees) {
			return n.dependees[0], true
		}
	}
	return 0, false
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
