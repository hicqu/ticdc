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
	// map[string]*slot
	slots    sync.Map
	numSlots int64
}

// NewSlots creates a new Slots.
func NewSlots[E Eq[E]](numSlots int64) *Slots[E] {
	return &Slots[E]{
		slots:    sync.Map{},
		numSlots: numSlots,
	}
}

// Add adds an elem to the slots and calls onConflict for each case
// where elem is conflicting with an existing element.
// Note that onConflict can be called multiple times with the same
// dependee.
func (s *Slots[E]) Add(elem E, keys []string, onConflict func(dependee E)) {
	for _, key := range keys {
		needInsert := true

		value, _ := s.slots.LoadOrStore(key, &slot{elems: nil})
		elemList := value.(*slot)
		elemList.mu.Lock()
		if elemList.elems == nil {
			elemList.elems = list.New()
		} else {
			lastElem := elemList.elems.Back().Value.(E)
			if lastElem.Equals(elem) {
				needInsert = false
			} else {
				onConflict(lastElem)
			}
		}

		if needInsert {
			elemList.elems.PushBack(elem)
		}
		elemList.mu.Unlock()
	}
}

// Remove removes an element from the Slots.
func (s *Slots[E]) Remove(elem E, keys []string) {
	for _, key := range keys {
		value, _ := s.slots.Load(key)
		if value == nil {
			panic("elem list is not found")
		}
		elemList := value.(*slot)
		elemList.mu.Lock()
		for e := elemList.elems.Front(); e != nil; e = e.Next() {
			if elem.Equals(e.Value.(E)) {
				elemList.elems.Remove(e)
				break
			}
		}
		elemList.mu.Unlock()
	}
}

type slot struct {
	elems *list.List
	mu    sync.Mutex
}
