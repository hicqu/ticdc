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

package txn

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type Txn interface {
	// Keys should be deduped.
	ConflictKeys(slots uint64) []uint64
}

type CasusalityWorker[T Txn] interface {
	Add(txn T, unlock func())
}

type Causality[T Txn] struct {
	slots     uint64
	relations []*node[T]
	notifier  chan struct{}
	workers   []CasusalityWorker[T]
}

func NewCausality[T Txn](workers []CasusalityWorker[T], slots uint64) *Causality[T] {
	return &Causality[T]{
		slots:     slots,
		relations: make([]*node[T], slots),
		notifier:  make(chan struct{}, 1),
		workers:   workers,
	}
}

func (c *Causality[T]) Add(txn T) {
	keys := txn.ConflictKeys(c.slots)

	var conflict bool
	var workerIdx int
	for {
		// TODO: maybe can avoid redundant check.
		conflict, workerIdx = c.detectConflicts(keys)
		if !conflict {
			workerIdx = c.randWorker()
			break
		}
		if workerIdx == multipleConflicting {
			<-c.notifier
			continue
		}
	}

	node := &node[T]{assignedTo: workerIdx}
	for _, key := range keys {
		slotAddr := (*unsafe.Pointer)(unsafe.Pointer(&c.relations[key]))
		atomic.StorePointer(slotAddr, unsafe.Pointer(node))
	}

	c.workers[workerIdx].Add(txn, func() {
		for _, key := range txn.ConflictKeys(c.slots) {
			slotAddr := (*unsafe.Pointer)(unsafe.Pointer(&c.relations[key]))
			atomic.CompareAndSwapPointer(slotAddr, unsafe.Pointer(node), nil)
		}
		node.mu.Lock()
		node.mu.Unlock()
		node.notified = true
		if node.notifier != nil {
			node.notifier.notifier <- struct{}{}
		}
	})
}

const (
	noConflicting       = -1
	multipleConflicting = -2
)

type node[T Txn] struct {
	assignedTo int

	mu       sync.Mutex
	notified bool
	notifier *Causality[T]
}

func (n *node[T]) setNotifier(c *Causality[T]) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.notified {
		n.notifier = c
		return true
	}
	return false
}

// detectConflict detects if there is a conflict between
// the keys of this txn and the worker's other txn.
// It will have several scenarios:
// 1) no conflict, return (false, noConflicting)
// 2) conflict with the same worker, return (true, workerIndex)
// 3) conflict with multiple workers, return (true, multipleConflicting)
func (c *Causality[T]) detectConflicts(keys []uint64) (bool, int) {
	if len(keys) == 0 {
		return false, noConflicting
	}

	conflictingWorkerIndex := noConflicting
	for _, key := range keys {
		slotAddr := (*unsafe.Pointer)(unsafe.Pointer(&c.relations[key]))
		if x := atomic.LoadPointer(slotAddr); x != nil {
			node := (*node[T])(x)
			if conflictingWorkerIndex == noConflicting {
				// The first conflict occurred.
				conflictingWorkerIndex = node.assignedTo
			} else if conflictingWorkerIndex != node.assignedTo {
				// A second conflict occurs, and it is with another worker.
				// For example:
				// txn0[a,b,c] --> worker0
				// txn1[t,f] --> worker1
				// txn2[a,f] --> ?
				// In this case, if we distribute the transaction,
				// there is no guarantee that it will be executed
				// in the order it was sent to the worker,
				// so we have to wait for the previous transaction to finish writing.
				if node.setNotifier(c) {
					return true, multipleConflicting
				}
			}
		}
	}

	// 1) no conflict
	// 2) conflict with the same worker
	return conflictingWorkerIndex != noConflicting, conflictingWorkerIndex
}

func (c *Causality[T]) randWorker() int {
	return 0
}
