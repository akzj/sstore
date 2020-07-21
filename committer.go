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
	sizeMap             *int64LockMap

	immutableMStreamMaps          []*mStreamTable
	maxImmutableMStreamTableCount int
	locker                        *sync.RWMutex

	flusher *flusher

	segments       map[string]*segment
	segmentsLocker *sync.RWMutex

	indexTable  *indexTable
	endWatchers *endWatchers
}

func newCommitter(options Options,
	endWatchers *endWatchers,
	indexTable *indexTable,
	segments map[string]*segment,
	sizeMap *int64LockMap,
	mutableMStreamMap *mStreamTable,
	queue *entryQueue) *committer {
	return &committer{
		queue:                         queue,
		maxMStreamTableSize:           options.MaxMStreamTableSize,
		mutableMStreamMap:             mutableMStreamMap,
		sizeMap:                       sizeMap,
		immutableMStreamMaps:          make([]*mStreamTable, 0, 32),
		locker:                        new(sync.RWMutex),
		flusher:                       newFlusher(),
		segments:                      segments,
		segmentsLocker:                new(sync.RWMutex),
		indexTable:                    indexTable,
		endWatchers:                   endWatchers,
		maxImmutableMStreamTableCount: options.MaxImmutableMStreamTableCount,
	}
}

func (c *committer) appendSegment(filename string, segment *segment) {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment.refInc()
	c.segments[filename] = segment
	c.indexTable.update1(segment)
}

func (c *committer) deleteSegment(filename string) error {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment, ok := c.segments[filename]
	if ok == false {
		return errNoFindSegment
	}
	delete(c.segments, filename)
	c.indexTable.remove1(segment)
	segment.refDec()
	return nil
}

func (c *committer) flushCallback(filename string, table *mStreamTable) {
	segment, err := openSegment(filename)
	if err != nil {
		log.Fatal(err.Error())
	}
	c.locker.Lock()
	if c.immutableMStreamMaps[0] != table {
		panic("flushCallback error")
	}
	if len(c.immutableMStreamMaps) > c.maxImmutableMStreamTableCount {
		c.immutableMStreamMaps[0] = nil
		c.immutableMStreamMaps = c.immutableMStreamMaps[1:]
	}
	c.locker.Unlock()

	c.appendSegment(filename, segment)
	//update indexTable
	for _, mStream := range table.mStreams {
		c.indexTable.remove(mStream)
	}
}

func (c *committer) getAllMStreamTable() []*mStreamTable {
	c.locker.RLock()
	var tables = make([]*mStreamTable, len(c.immutableMStreamMaps)+1)
	tables = append(tables, c.mutableMStreamMap)
	tables = append(tables, c.immutableMStreamMaps...)
	c.locker.RUnlock()
	return tables
}

func (c *committer) flush() {
	mStreamMap := c.mutableMStreamMap
	c.locker.Lock()
	c.immutableMStreamMaps = append(c.immutableMStreamMaps, c.mutableMStreamMap)
	c.mutableMStreamMap = newMStreamTable(c.sizeMap, len(c.mutableMStreamMap.mStreams))
	c.locker.Unlock()
	c.flusher.append(mStreamMap, func(segmentFile string, err error) {
		if err != nil {
			log.Fatal(err.Error())
		}
		c.flushCallback(segmentFile, mStreamMap)
	})
}

func (c *committer) getMutableMStreamTable() *mStreamTable {
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.mutableMStreamMap
}

func (c *committer) start() {
	go func() {
		for {
			entries := c.queue.take()
			mStreamTable := c.getMutableMStreamTable()
			for i := range entries {
				entry := entries[i]
				mStream, end := mStreamTable.appendEntry(entry)
				if mStream != nil {
					c.indexTable.commit(func() {
						c.indexTable.update(mStream)
					})
				}

				item := notifyItemPool.Get().(*notifyItem)
				item.name = entry.name
				item.end = end
				c.endWatchers.notify(item)

				if c.mutableMStreamMap.mSize >= c.maxMStreamTableSize {
					c.flush()
				}
			}
		}
	}()
}

type mStreamTable struct {
	locker      sync.Mutex
	mSize       int64
	lastEntryID int64
	endMap      *int64LockMap
	GcTS        time.Time
	mStreams    map[string]*mStream
	indexTable  *indexTable
}

func newMStreamTable(sizeMap *int64LockMap, mStreamMapSize int) *mStreamTable {
	return &mStreamTable{
		mSize:       0,
		lastEntryID: 0,
		endMap:      sizeMap,
		locker:      sync.Mutex{},
		mStreams:    make(map[string]*mStream, mStreamMapSize),
	}
}

func (m *mStreamTable) loadOrCreateMStream(name string) (*mStream, bool) {
	m.locker.Lock()
	ms, ok := m.mStreams[name]
	if ok {
		m.locker.Unlock()
		return ms, true
	}
	size, _ := m.endMap.get(name)
	ms = newMStream(size, name)
	m.mStreams[name] = ms
	m.locker.Unlock()
	return ms, false
}

//appendEntry append entry mStream,and return the mStream if it created
func (m *mStreamTable) appendEntry(e *entry) (*mStream, int64) {
	ms, load := m.loadOrCreateMStream(e.name)
	end := ms.write(e.data)
	m.endMap.set(e.name, end)
	m.mSize += int64(len(e.data))
	m.lastEntryID = e.ID
	if load {
		return nil, end
	}
	return ms, end
}

type int64LockMap struct {
	locker *sync.RWMutex
	sizes  map[string]int64
}

func newInt64LockMap() *int64LockMap {
	return &int64LockMap{
		locker: new(sync.RWMutex),
		sizes:  make(map[string]int64, 1024),
	}
}

func (sizeMap *int64LockMap) set(name string, pos int64) {
	sizeMap.locker.Lock()
	sizeMap.sizes[name] = pos
	sizeMap.locker.Unlock()
}

func (sizeMap *int64LockMap) get(name string) (int64, bool) {
	sizeMap.locker.RLock()
	size, ok := sizeMap.sizes[name]
	sizeMap.locker.RUnlock()
	return size, ok
}
