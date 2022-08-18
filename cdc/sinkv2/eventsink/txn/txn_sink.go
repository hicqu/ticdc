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
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/backends"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/causality"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector.
	DefaultConflictDetectorSlots int64 = 1024 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the sink for SingleTableTxn.
type sink struct {
	conflictDetector *causality.ConflictDetector[*worker, *txnEvent]
	workers          []*worker
}

func NewSink(ctx context.Context, backends []backends.Backend, errCh chan<- error, conflictDetectorSlots int64) *sink {
	workers := make([]*worker, 0, len(backends))
    log.Info("QP txn sink workers", zap.Int("count", len(backends)))
	for i, backend := range backends {
		w := newWorker(ctx, i, backend, errCh)
		w.runBackgroundLoop()
		workers = append(workers, w)
	}
	detector := causality.NewConflictDetector[*worker, *txnEvent](workers, conflictDetectorSlots)
	return &sink{conflictDetector: detector, workers: workers}
}

// WriteEvents writes events to the sink.
func (s *sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) (err error) {
    log.Info("[QP] txnsink.WriteEvents is called", zap.Int("events", len(rows)))
	for _, row := range rows {
        log.Info("[QP] txnsink is going to Add an event into conflictDetector")
		err = s.conflictDetector.Add(newTxnEvent(row))
        log.Info("[QP] txnsink finishes to Add an event into conflictDetector")
		if err != nil {
			return
		}
	}
	return
}

// Close closes the sink. It won't wait for all pending items backend handled.
func (s *sink) Close() error {
	s.conflictDetector.Close()
	for _, w := range s.workers {
		w.Close()
	}
	return nil
}
