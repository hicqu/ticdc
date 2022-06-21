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

package tablesink

import (
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/roweventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/txneventsink"
	"github.com/uber-go/atomic"
)

// TableSink is the interface for table sink.
// It is used to sink data in table units.
type TableSink interface {
	// AppendRowChangedEvents appends row changed events to the table sink.
	// Usually, it is used to cache the row changed events into table sink.
	AppendRowChangedEvents(rows ...*model.RowChangedEvent) error
	// UpdateResolvedTs writes the buffered row changed events to the TxnEventSink/RowEventSink.
	// Note: This is an asynchronous method.
	UpdateResolvedTs(resolvedTs model.ResolvedTs) error
	// GetCheckpointTs returns the current checkpoint ts of table sink.
	// Usually, it requires some computational work.
	// For example, calculating the current progress from the minimum heap.
	GetCheckpointTs() (model.ResolvedTs, error)
	// Close closes the table sink.
	// 1) set the table status to stopped.
	// 2) clean up the table sink buffer.
	Close() error
}

type TableSinkState struct {
	// stopped is used to ensure no more events can be flushed to downstreams if table sink is stopped.
	stopped atomic.Bool

	// In backends, events can be partitioned into several workers. Every worker should be assigned
	// an ID starts at 0. Then every worker can update its own progress.
	partitionedProgress []atomic.Uint64

	// Causality isn't thread-safe.
	causality Causality
}

type PartitionedProgress struct {
    partitions sync.Map
    lock sync.Mutex // only used for create new partitions.
}

func (p *PartitionedProgress) maybeCreatePartition(id string) {
    if _, ok := p.partitions[id]; !ok {
        p.lock.Lock()
        defer func () {p.lock.Unlock()}()
    }
    if _, ok := p.partitions[id]; ok {
        return
    }
    mints := math.MaxUint64
    for ts := range p.partitions {
        if mints > ts {
            mints = ts
        }
    }
    // Events come from one stream and they are ordered. So when split them into N+1 ways,
    // the new added one can be assigned the minimal ts in all partitions.
    p.partitions[id] = mints
}

func (p *PartitionedProgress) postFlush(id string, ts uint64) {
    p.partitions[id] = ts
}

type tableSinkImpl struct {
	txnBackend txneventsink.TxnEventSink
	rowBackend roweventsink.RowEventSink
	txnBuffer  *model.SingleTableTxn // Maybe []*model.SingleTableTxn
	rowBuffer  []*model.RowChangedEvent
	state      TableSinkState
}

func newTableSink() TableSink {
	var txnBackend txneventsink.TxnEventSink = nil
	txnBackend.WorkerCount
	return &tableSinkImpl{
		txnBackend: txnBackend,
		state:      TableSinkState{partitionedProgress: make([]atomic.Uint64, txnBackend.WorkerCount())},
	}
}

func (t *tableSinkImpl) AppendRowChangedEvents(rows ...*model.RowChangedEvent) (err error) {
	if t.txnBackend != nil {
		// Merge events into t.txnBuffer
	}
	if t.rowBackend != nil {
		// Merge events into t.rowBuffer
	}
	return
}

func (t *tableSinkImpl) UpdateResolvedTs(resolvedTs model.ResolvedTs) (err error) {
	if t.txnBackend != nil {
		event := txneventsink.TxnEvent{
			Txn:            t.txnBuffer,
            stopped: &t.state.stopped,
            postFlush: func(workerID int, ts uint64) {
                // Here we suppose workerIDs are continuous. If they are not, we can use a hash map
                // instead of slice.
                t.state.partitionedProgress[workerID].Store(ts)
            },
		}
		t.txnBackend.WriteTxnEvents(event)
	}
	return
}

func (t *tableSinkImpl) GetCheckpointTs() (rts model.ResolvedTs, err error) {
	t.state.RLock()
	defer t.state.RUnlock()
	// return min(t.state.partitionedProgress), nil
	return
}

func (t *tableSinkImpl) Close() (err error) {
	return
}

// It's copied from cdc/sink/mysql/causality.go.
type Causality struct {
	relations map[string]int
}
