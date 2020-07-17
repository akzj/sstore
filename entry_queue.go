// Copyright 2020-2026 The sstore Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sstore

import "sync"

type entryQueue struct {
	cap     int
	locker  *sync.Mutex
	empty   *sync.Cond
	full    *sync.Cond
	entries []*entry
}

func newEntryQueue(cap int) *entryQueue {
	locker := new(sync.Mutex)
	return &entryQueue{
		cap:     cap,
		locker:  locker,
		empty:   sync.NewCond(locker),
		full:    sync.NewCond(locker),
		entries: make([]*entry, 64),
	}
}
func (queue *entryQueue) take() []*entry {
	queue.locker.Lock()
	for len(queue.entries) == 0 {
		queue.empty.Wait()
	}
	entries := queue.entries
	queue.entries = entriesPool.Get().([]*entry)[:0]
	queue.entries = queue.entries[:0]
	queue.locker.Unlock()
	queue.full.Signal()
	return entries
}

func (queue *entryQueue) put(e *entry) {
	queue.locker.Lock()
	for len(queue.entries) > queue.cap {
		queue.full.Wait()
	}
	queue.entries = append(queue.entries, e)
	queue.empty.Signal()
	queue.locker.Unlock()
}

func (queue *entryQueue) putEntries(entries []*entry) {
	queue.locker.Lock()
	for len(queue.entries) > queue.cap {
		queue.full.Wait()
	}
	queue.entries = append(queue.entries, entries...)
	queue.empty.Signal()
	queue.locker.Unlock()
}
