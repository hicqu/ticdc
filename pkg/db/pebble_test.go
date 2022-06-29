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

package db

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIteratorWithTableFilter(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(context.Background(), 1, dbPath, &config.DBConfig{Count: 1},
		WithCache(16<<20), WithTableCRTsCollectors(),
		// Disable auto compactions to make the case more stable.
		func(opts *pebble.Options) { opts.DisableAutomaticCompactions = true },
	)
	if err != nil {
		errmsg := fmt.Sprintf("OpenPebble fail: %v", err)
		panic(errmsg)
	}
	defer func() { db.Close() }()

	// Put 7 table keys with CRTS=1, and then flush it to L0. The flush is required for generating table properties.
	for t := 1; t <= 7; t++ {
		key := encoding.EncodeTsKey(1, uint64(t), 1)
		b := db.Batch(1024)
		b.Put(key, []byte{'x'})
		b.Commit()
	}
	if err = db.(*pebbleDB).db.Flush(); err != nil {
		errmsg := fmt.Sprintf("Flush fail: %v", err)
		panic(errmsg)
	}

	// Put 9 table keys with CRTS=3, and then flush it to L0.
	for t := 1; t <= 9; t++ {
		key := encoding.EncodeTsKey(1, uint64(t), 3)
		b := db.Batch(1024)
		b.Put(key, []byte{'x'})
		b.Commit()
	}
	if err = db.(*pebbleDB).db.Flush(); err != nil {
		errmsg := fmt.Sprintf("Flush fail: %v", err)
		panic(errmsg)
	}

	// Sleep a while. Automatic compactions shouldn't be triggered.
	time.Sleep(time.Second)

	// There should be no any compactions but 2 tables at L0.
	pebbleInstance := db.(*pebbleDB).db
	stats := pebbleInstance.Metrics()
	require.Equal(t, int64(0), stats.Compact.Count)
	require.Equal(t, int64(2), stats.Levels[0].NumFiles)
	// 7 is a pebble internal constant.
	// See: https://github.com/cockroachdb/pebble/blob/
	// 71d17c2a007bfad5111a229ba325d30251b88a41/internal/manifest/version.go#L579
	for level := 1; level < 7; level++ {
		require.Equal(t, int64(0), stats.Levels[level].NumFiles)
	}

	for _, x := range []struct {
		lowerTs, upperTs uint64
		expectedCount    int
	}{
		{lowerTs: 0, upperTs: 1, expectedCount: 7},
		{lowerTs: 1, upperTs: 2, expectedCount: 7},
		{lowerTs: 2, upperTs: 3, expectedCount: 9},
		{lowerTs: 3, upperTs: 4, expectedCount: 9},
		{lowerTs: 0, upperTs: 10, expectedCount: 16},
		{lowerTs: 10, upperTs: 20, expectedCount: 0},
	} {
		iter := db.Iterator(encoding.EncodeTsKey(1, 0, 0), encoding.EncodeTsKey(1, 10, 0), x.lowerTs, x.upperTs)
		require.False(t, iter.Valid())
		count := 0
		valid := iter.Seek(encoding.EncodeTsKey(1, 0, 0))
		for valid {
			count += 1
			valid = iter.Next()
		}
		require.Equal(t, x.expectedCount, count)
	}
}
