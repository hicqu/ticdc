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
	"sync"
)

// SlotNode describes objects that can be compared for equality.
type SlotNode[T any] interface {
	AllocID()
	// NodeID tells the node's ID.
	NodeID() int64
	// Construct a dependency on `others`.
	DependOn(others map[int64]T)
	// Remove the node itself and notify all dependers.
	Remove()
	// Free the node itself and remove it from the graph.
	Free()
}

// Slots implements slot-based conflict detection.
// It holds references to E, which can be used to build
// a DAG of dependency.
type Slots[E SlotNode[E]] struct {
	slots map[string]*E
	mu    sync.Mutex
}

// NewSlots creates a new Slots.
func NewSlots[E SlotNode[E]]() *Slots[E] {
	return &Slots[E]{
		slots: make(map[string]*E, 1024),
	}
}

// Add adds an elem to the slots and calls onConflict for each case
// where elem is conflicting with an existing element.
// Note that onConflict can be called multiple times with the same
// dependee.
func (s *Slots[E]) Add(elem E, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dependOnList := make(map[int64]E, len(keys))
	for _, key := range keys {
		dep := s.slots[key]
		if dep == nil {
			dep = new(E)
			s.slots[key] = dep
		} else {
			prevID := (*dep).NodeID()
			dependOnList[prevID] = *dep
		}
		*dep = elem
	}
	elem.AllocID()
	elem.DependOn(dependOnList)
}

// Free removes an element from the Slots.
func (s *Slots[E]) Free(elem E, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		dep := s.slots[key]
		if dep != nil && (*dep).NodeID() == elem.NodeID() {
			delete(s.slots, key)
		}
	}
	elem.Free()
}

type slot[E SlotNode[E]] struct {
	tail *E
}
