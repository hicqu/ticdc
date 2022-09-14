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

package causality

import (
	"sync"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality/internal"
	"go.uber.org/atomic"
)

// ConflictDetector implements a logic that dispatches transaction
// to different workers in a way that transactions modifying the same
// keys are never executed concurrently and have their original orders
// preserved.
type ConflictDetector[Worker worker[Txn], Txn txnEvent] struct {
	workers []Worker

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots    *internal.Slots[*internal.Node]
	numSlots int64

	// nextWorkerID is used to dispatch transactions round-robin.
	nextWorkerID atomic.Int64

	// For a uackground loop to GC nodes.
	garbageNodes *containers.SliceQueue[txnFinishedEvent[Txn]]
	wg           sync.WaitGroup
	closeCh      chan struct{}
}

type txnFinishedEvent[Txn txnEvent] struct {
	txn  Txn
	node *internal.Node
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	numSlots int64,
) *ConflictDetector[Worker, Txn] {
	ret := &ConflictDetector[Worker, Txn]{
		workers:      workers,
		slots:        internal.NewSlots[*internal.Node](numSlots),
		numSlots:     numSlots,
		garbageNodes: containers.NewSliceQueue[txnFinishedEvent[Txn]](),
		closeCh:      make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.runBackgroundTasks()
	}()

	return ret
}

// Add pushes a transaction to the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Add(txn Txn) error {
	node := internal.NewNode()
	node.OnResolved = func(workerID int64) {
		unlock := func() {
			node.Remove()
			d.slots.Free(node, txn.ConflictKeys(d.numSlots))
		}
		d.sendToWorker(txn, unlock, workerID)
	}
	node.RandWorkerID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.workers)) }
	d.slots.Add(node, txn.ConflictKeys(d.numSlots))
	return nil
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Worker, Txn]) runBackgroundTasks() {
	for {
		select {
		case <-d.closeCh:
			return
		case <-d.garbageNodes.C:
			for {
				event, ok := d.garbageNodes.Pop()
				if !ok {
					break
				}
				d.slots.Free(event.node, event.txn.ConflictKeys(d.numSlots))
			}
		}
	}
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Worker, Txn]) sendToWorker(txn Txn, unlock func(), workerID int64) {
	if workerID < 0 {
		panic("must assign with a valid workerID")
	}
	worker := d.workers[workerID]
	worker.Add(txn, unlock)
}
