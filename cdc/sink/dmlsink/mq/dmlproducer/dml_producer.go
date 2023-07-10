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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

// DMLProducer is the interface for message producer.
type DMLProducer interface {
	// AsyncSendMessage sends a message asynchronously.
	AsyncSendMessage(
		ctx context.Context, topic string, partition int32, message *common.Message,
	) error

	// Run the producer and client(s).
	Run(ctx context.Context) error

	// Close closes the producer and client(s).
	// Must be called after `Run` returns.
	Close()
}

// Factory is a function to create a producer.
type Factory func(
	changefeedID model.ChangeFeedID,
	asyncProducer kafka.AsyncProducer,
	metricsCollector kafka.MetricsCollector,
	failpointCh chan error,
) DMLProducer
