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

package config

import cerror "github.com/pingcap/ticdc/pkg/errors"

// SorterConfig represents sorter config for a changefeed
type SorterConfig struct {
	// number of concurrent heap sorts
	NumConcurrentWorker int `toml:"num-concurrent-worker" json:"num-concurrent-worker"`
	// maximum size for a heap
	ChunkSizeLimit uint64 `toml:"chunk-size-limit" json:"chunk-size-limit"`
	// the maximum memory use percentage that allows in-memory sorting
	MaxMemoryPressure int `toml:"max-memory-percentage" json:"max-memory-percentage"`
	// the maximum memory consumption allowed for in-memory sorting
	MaxMemoryConsumption uint64 `toml:"max-memory-consumption" json:"max-memory-consumption"`
	// the size of workerpool
	NumWorkerPoolGoroutine int `toml:"num-workerpool-goroutine" json:"num-workerpool-goroutine"`
	// the directory used to store the temporary files generated by the sorter
	SortDir string `toml:"sort-dir" json:"sort-dir"`
}

// ValidateAndAdjust validates and adjusts the sorter configuration
func (c *SorterConfig) ValidateAndAdjust() error {
	if c.ChunkSizeLimit < 1*1024*1024 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("chunk-size-limit should be at least 1MB")
	}
	if c.NumConcurrentWorker < 1 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("num-concurrent-worker should be at least 1")
	}
	if c.NumWorkerPoolGoroutine > 4096 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("num-workerpool-goroutine should be at most 4096")
	}
	if c.NumConcurrentWorker > c.NumWorkerPoolGoroutine {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("num-concurrent-worker larger than num-workerpool-goroutine is useless")
	}
	if c.NumWorkerPoolGoroutine < 1 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("num-workerpool-goroutine should be at least 1, larger than 8 is recommended")
	}
	if c.MaxMemoryPressure < 0 || c.MaxMemoryPressure > 100 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("max-memory-percentage should be a percentage")
	}

	return nil
}
