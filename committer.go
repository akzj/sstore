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

import (
	"log"
	"sync"
	"time"
)

type committer struct {
	queue *entryQueue

	maxMStreamTableSize int64
	mutableMStreamMap   *mStreamTable
	sizeMap             *sizeMap

	immutableMStreamMaps []*mStreamTable

	locker sync.Mutex

	flusher *flusher
}

type sizeMap struct {
	locker *sync.RWMutex
	sizes  map[string]int64
}

func (sizeMap *sizeMap) set(name string, pos int64) {
	sizeMap.locker.Lock()
	sizeMap.sizes[name] = pos
	sizeMap.locker.Unlock()
}

func (sizeMap *sizeMap) get(name string) (int64, bool) {
	sizeMap.locker.RLock()
	size, ok := sizeMap.sizes[name]
	sizeMap.locker.RUnlock()
	return size, ok
}

type mStreamTable struct {
	locker      sync.Mutex
	mSize       int64
	lastEntryID int64
	sizeMap     *sizeMap
	GcTS        time.Time
	mStreams    map[string]*mStream
}

func newMStreamTable(sizeMap *sizeMap, mStreamMapSize int) *mStreamTable {
	return &mStreamTable{
		mSize:       0,
		lastEntryID: 0,
		sizeMap:     sizeMap,
		locker:      sync.Mutex{},
		mStreams:    make(map[string]*mStream, mStreamMapSize),
	}
}

func (m *mStreamTable) getMStream(name string) *mStream {
	m.locker.Lock()
	ms, ok := m.mStreams[name]
	if ok {
		m.locker.Unlock()
		return ms
	}
	size, _ := m.sizeMap.get(name)
	ms = newMStream(size, name)
	m.mStreams[name] = ms
	m.locker.Unlock()
	return ms
}

func (m *mStreamTable) appendEntry(e *entry) {
	ms := m.getMStream(e.name)
	n := ms.Write(e.data)
	m.sizeMap.set(e.name, n)
	m.mSize += int64(len(e.data))
	m.lastEntryID = e.ID
}

func (c *committer) flushCallback(table *mStreamTable) {
	c.locker.Lock()
	if c.immutableMStreamMaps[0] != table {
		panic("flushCallback error")
	}
	c.immutableMStreamMaps = c.immutableMStreamMaps[1:]
	c.locker.Unlock()
}

func (c *committer) flush() {
	toFlush := c.mutableMStreamMap
	c.locker.Lock()
	c.immutableMStreamMaps = append(c.immutableMStreamMaps, c.mutableMStreamMap)
	c.mutableMStreamMap = newMStreamTable(c.sizeMap, len(c.mutableMStreamMap.mStreams))
	c.locker.Unlock()
	c.flusher.append(toFlush, func(err error) {
		if err != nil {
			log.Fatal(err.Error())
		}
		c.flushCallback(toFlush)
	})
}

func (c *committer) start() {
	go func() {
		for {
			entries := c.queue.take()
			for i := range entries {
				c.mutableMStreamMap.appendEntry(entries[i])
				if c.mutableMStreamMap.mSize >= c.maxMStreamTableSize {
					c.flush()
				}
			}
		}
	}()
}
