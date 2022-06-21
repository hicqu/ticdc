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

package roweventsink

import (
	"go.uber.org/atomic"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
)

// RowEvent represents a row event with callbacks.
// In addition, it contains the state of the table.
// When we process row events, TableStopped is used to
// determine if we really need to process the event.
type RowEvent struct {
	Row          *model.RowChangedEvent
	TableStopped *atomic.Bool
    partitionedProgress *tablesink.PartitionedProgress
}

// RowEventSink is a sink that processes row events.
// Usually, it is a MQ sink or S3 sink(not implemented).
type RowEventSink interface {
	// WriteRowChangedEvents writes row changed events to the sink.
	// Note: This is an asynchronous and thread-safe method.
    // postFlush should be thread-safe.
	WriteRowChangedEvents(rows ...*RowEvent) error
	// Close closes the sink.
	Close() error
}

type rowEventSinkImpl struct {
    input chan<-*RowEvent
    eventRouter *dispatcher.EventRouter
	topicManager         manager.TopicManager
	flushWorker          *flushWorker
}

func (r *rowEventSinkImpl) WriteRowChangedEvents(rows ...*RowEvent) error {
    for _, row := range rows {
        r.input <- rows
    }
	for _, row := range rows {
		topic := r.eventRouter.GetTopicForRowChange(row)
		partitionNum, err := r.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := r.eventRouter.GetPartitionForRowChange(row, partitionNum)

        id := fmt.Sprintf("%s_%d", topic, partition)
        row.partitionedProgress.maybeCreatePartition(id)

		err = k.flushWorker.addEvent(ctx, mqEvent{
			row: row,
			key: topicPartitionKey{ topic: topic, partition: partition },
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type partitionedProgressKey struct {
    id string
    progress *tablesink.PartitionedProgress
}

// Copied from cdc/sink/mq/mq_flush_worker.go
func (w *flushWorker) run(ctx context.Context) (retErr error) {
	defer func() {
		w.ticker.Stop()
		log.Info("flushWorker exited", zap.Error(retErr))
	}()
	eventsBuf := make([]mqEvent, flushBatchSize)


    var partitionedProgress = make(map[partitionedProgressKey]uint64, 1024)
	for {
        var event mqEvent
        var endIndex int
        var err error
		row, endIndex, err = w.batch(ctx, eventsBuf) // row is *Row.Event
		if err != nil {
			return errors.Trace(err)
		}
		if endIndex == 0 {
			if w.needsFlush != nil {
				if err := w.flushAndNotify(ctx, partitionedProgress); err != nil {
					return errors.Trace(err)
				}
			}
			continue
		}
		msgs := eventsBuf[:endIndex]
		partitionedRows := w.group(msgs)
		err = w.asyncSend(ctx, partitionedRows)
        key := partitionedProgressKey {
            id: fmt.Sprintf("%s_%d", event.key.topic, event.key.partition),
            progress: event.row.partitionedProgress
        }
        partitionedProgress[key] = event.row.Row.CommitTs // they are monotonically increasing.
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// Copied from cdc/sink/mq/mq_flush_worker.go, and add someting.
func (w *flushWorker) flushAndNotify(ctx context.Context, partitionedProgress map[partitionedProgressKey]uint64) error {
	start := time.Now()
	err := w.producer.Flush(ctx)
	if err != nil {
		return err
	}

    for key, ts := range partitionedProgress {
        progress.postFlush(key.id, ts)
    }

    // TODO: needsFlush can be removed. flushWorker can determine whether to flush or not.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.needsFlush <- struct{}{}:
		close(w.needsFlush)
		// NOTICE: Do not forget to reset the needsFlush.
		w.needsFlush = nil
		log.Debug("flush worker flushed", zap.Duration("duration", time.Since(start)))
	}

	return nil
}
