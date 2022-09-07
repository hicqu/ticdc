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

package internal

import (
	"container/list"
	"sync"
)

// Eq describes objects that can be compared for equality.
type Eq[T any] interface {
	Equals(other T) bool
}

// Slots implements slot-based conflict detection.
// It holds references to E, which can be used to build
// a DAG of dependency.
type Slots[E Eq[E]] struct {
	slots    []slot
	numSlots int64
}

// NewSlots creates a new Slots.
func NewSlots[E Eq[E]](numSlots int64) *Slots[E] {
	return &Slots[E]{
		slots:    make([]slot, numSlots),
		numSlots: numSlots,
	}
}

// Add adds an elem to the slots and calls onConflict for each case
// where elem is conflicting with an existing element.
// Note that onConflict can be called multiple times with the same
// dependee.
func (s *Slots[E]) Add(elem E, keys []int64, onConflict func(dependee E)) {
	for _, key := range keys {
		if i > 0 && keys[i-1] == key {
			continue
		}
		s.slots[key].mu.Lock()
		if s.slots[key].elems == nil {
			s.slots[key].elems = list.New()
			s.slots[key].elems.PushBack(elem)
		} else {
			lastElem := s.slots[key].elems.Back().Value.(E)
			if !lastElem.Equals(elem) {
				onConflict(lastElem)
				s.slots[key].elems.PushBack(elem)
			}
		}
	}
	// Lock those slots one by one and then unlock them one by one, so that
	// we can avoid 2 transactions get executed interleaved.
	for _, key := range keys {
		if i > 0 && keys[i-1] == key {
			continue
		}
		s.slots[key].mu.Unlock()
	}
}

// Remove removes an element from the Slots.
func (s *Slots[E]) Remove(elem E, keys []int64) {
LOOP:
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			continue
		}
		s.slots[key].mu.Lock()
		if s.slots[key].elems != nil {
			for e := s.slots[key].elems.Front(); e != nil; e = e.Next() {
				if elem.Equals(e.Value.(E)) {
					s.slots[key].elems.Remove(e)
					if s.slots[key].elems.Len() == 0 {
						s.slots[key].elems = nil
					}
					continue LOOP
				}
			}
		}
		panic("elem should always be found")
	}
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			continue
		}
		s.slots[key].mu.Unlock()
	}
}

type slot struct {
	elems *list.List
	mu    sync.Mutex
}
