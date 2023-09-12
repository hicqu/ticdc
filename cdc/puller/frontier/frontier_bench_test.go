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

package frontier

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
)

func toCMPBytes(i int) []byte {
	s := fmt.Sprintf("t_XXXXXXXX_r_%09d", i)
	return []byte(s)
}

func BenchmarkSpanForwardAndFrontier(b *testing.B) {
	tests := []struct {
		order   string
		regions int
		rounds  int
	}{
		{"random", 900_000, 10},
		{"random", 400_000, 10},
		{"ordered", 900_000, 10},
		{"ordered", 400_000, 10},
	}

	for _, test := range tests {
		order := test.order
		regions := test.regions
		rounds := test.rounds

		spans := make([]tablepb.Span, 0, regions)
		for i := 0; i < regions; i++ {
			span := tablepb.Span{StartKey: toCMPBytes(i), EndKey: toCMPBytes(i + 1)}
			spans = append(spans, span)
		}
		totalSpan := tablepb.Span{StartKey: spans[0].StartKey, EndKey: spans[len(spans)-1].EndKey}
		f := NewFrontier(0, totalSpan)

		offsets := make([]uint64, 0, regions*rounds)
		if order == "random" {
			r := rand.New(rand.NewSource(time.Now().Unix()))
			for i := 0; i < regions*rounds; i++ {
				offsets = append(offsets, r.Uint64()%uint64(regions))
			}
		} else {
			for i := 0; i < regions*rounds; i++ {
				offsets = append(offsets, uint64(i)%uint64(regions))
			}
		}

		b.ResetTimer()
		b.Run(fmt.Sprintf("%s-%d(region)-%d(round)", order, regions, rounds), func(b *testing.B) {
			for i := 0; i < regions*rounds; i++ {
				offset := offsets[i]
				span := spans[offset]
				if spanz.IsSubSpan(span, totalSpan) {
					f.Forward(offset, span, offset)
					if i%regions == 0 {
						f.Frontier()
					}
				}
			}
		})
	}
}

func BenchmarkSpanFrontierOverlap(b *testing.B) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "5k", n: 5000},
		{name: "10k", n: 10_000},
		{name: "50k", n: 50_000},
		{name: "100k", n: 100_000},
	}

	steps := []int{5, 10, 100, 500}

	for _, test := range tests {
		n := test.n

		for _, step := range steps {
			b.Run(fmt.Sprintf("%s_%d", test.name, step), func(b *testing.B) {
				spans := make([]tablepb.Span, 0, n)
				forward := make([]tablepb.Span, 0, n)
				for i := 0; i < n; i++ {
					spans = append(spans, tablepb.Span{
						StartKey: toCMPBytes(i),
						EndKey:   toCMPBytes(i + 1),
					})
					forward = append(forward, tablepb.Span{
						StartKey: toCMPBytes(i),
						EndKey:   toCMPBytes(i + step),
					})
				}

				f := NewFrontier(0, spans...)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					f.Forward(0, forward[i%n], uint64(i))
				}
			})
		}
	}
}
