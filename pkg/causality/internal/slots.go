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

// SlotNode describes objects that can be compared for equality.
type SlotNode[T any] interface {
	// Equals tells whether two pointers to nodes are equal.
	Equals(other T) bool
	// Construct a dependency on `others`.
	DependOn(others []T)
	// Remove the node itself and notify all dependers.
	Remove()
	// Free the node itself and remove it from the graph.
	Free()
}

// Slots implements slot-based conflict detection.
// It holds references to E, which can be used to build
// a DAG of dependency.
type Slots[E SlotNode[E]] struct {
	slots    []slot
	numSlots int64
}

// NewSlots creates a new Slots.
func NewSlots[E SlotNode[E]](numSlots int64) *Slots[E] {
	return &Slots[E]{
		slots:    make([]slot, numSlots),
		numSlots: numSlots,
	}
}

// Add adds an elem to the slots and calls onConflict for each case
// where elem is conflicting with an existing element.
// Note that onConflict can be called multiple times with the same
// dependee.
func (s *Slots[E]) Add(elem E, keys []int64) {
	dependOnList := make([]E, 0, len(keys))
	for _, key := range keys {
		s.slots[key].mu.Lock()
		if s.slots[key].elems == nil {
			s.slots[key].elems = list.New()
			s.slots[key].elems.PushBack(elem)
		} else {
			lastElem := s.slots[key].elems.Back().Value.(E)
			dependOnList = append(dependOnList, lastElem)
			s.slots[key].elems.PushBack(elem)
		}
	}
	elem.DependOn(dependOnList)
	// Lock those slots one by one and then unlock them one by one, so that
	// we can avoid 2 transactions get executed interleaved.
	for _, key := range keys {
		s.slots[key].mu.Unlock()
	}
}

// Free removes an element from the Slots.
func (s *Slots[E]) Free(elem E, keys []int64) {
	for _, key := range keys {
		found := false
		s.slots[key].mu.Lock()
		if s.slots[key].elems != nil {
			for e := s.slots[key].elems.Front(); e != nil; e = e.Next() {
				// Keep removing garbage nodes until meet the target one.
				s.slots[key].elems.Remove(e)
				if elem.Equals(e.Value.(E)) {
					found = true
					break
				}
			}
			if s.slots[key].elems.Len() == 0 {
				s.slots[key].elems = nil
			}
		}
		s.slots[key].mu.Unlock()
		if !found {
			panic("should always find and remove slot header")
		}
	}
}

type slot struct {
	elems *list.List
	mu    sync.Mutex
}
