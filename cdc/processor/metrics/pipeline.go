// Copyright 2020 PingCAP, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	PullerResolvedTsLag = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "puller_lag",
			Help:      "Bucketed histogram of puller latency lag",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})

	UnifiedSorterResolvedTsLag = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "unified_sorter_lag",
			Help:      "Bucketed histogram of sorter latency lag",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})

	LeveldbSorterResolvedTsLag = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "leveldb_sorter_lag",
			Help:      "Bucketed histogram of sorter latency lag",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})
)

// InitMetrics init these metrics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PullerResolvedTsLag)
	registry.MustRegister(UnifiedSorterResolvedTsLag)
	registry.MustRegister(LeveldbSorterResolvedTsLag)
}
