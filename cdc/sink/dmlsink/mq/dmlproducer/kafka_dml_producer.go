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

package dmlproducer

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
)

var _ DMLProducer = (*kafkaDMLProducer)(nil)

// kafkaDMLProducer is used to send messages to kafka.
type kafkaDMLProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer kafka.AsyncProducer
	// metricsCollector is used to report metrics.
	metricsCollector kafka.MetricsCollector

	state struct {
		sync.RWMutex
		n int // 0(inited) -> 1(stopped) -> 2(closed).
	}

	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error
}

// NewKafkaDMLProducer creates a new kafka producer.
func NewKafkaDMLProducer(
	changefeedID model.ChangeFeedID,
	asyncProducer kafka.AsyncProducer,
	metricsCollector kafka.MetricsCollector,
	failpointCh chan error,
) DMLProducer {
	return &kafkaDMLProducer{
		id:               changefeedID,
		asyncProducer:    asyncProducer,
		metricsCollector: metricsCollector,
		failpointCh:      failpointCh,
	}
}

func (k *kafkaDMLProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	k.state.RLock()
	defer k.state.RUnlock()
	if k.state.n != stateInited {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("KafkaSinkAsyncSendError error injected", zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})
	return k.asyncProducer.AsyncSend(ctx, topic, partition,
		message.Key, message.Value, message.Callback)
}

// Run implements DMLProducer.
func (k *kafkaDMLProducer) Run(ctx context.Context) (err error) {
	log.Info("Starting kafka DML producer ...",
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID))
	defer func() {
		k.state.Lock()
		k.state.n = stateStopped
		k.state.Unlock()
		log.Info("Kafka DML producer exits",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID),
			zap.Error(err))
	}()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		k.metricsCollector.Run(ctx)
	}()

	err = k.asyncProducer.AsyncRunCallback(ctx)
	cancel()
	wg.Wait()
	return
}

// Close implements DMLProducer.
func (k *kafkaDMLProducer) Close() {
	k.state.Lock()
	defer k.state.Unlock()
	if k.state.n != stateClosed {
		k.state.n = stateClosed
		k.asyncProducer.Close()
		close(k.failpointCh)
	}
}

const (
	stateInited  int = 0
	stateStopped int = 1
	stateClosed  int = 2
)
