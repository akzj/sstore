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

	immutableMStreamMaps       []*mStreamTable
	maxImmutableMStreamMapSize int
	locker                     sync.RWMutex

	flusher *flusher

	segments       map[string]*segment
	segmentsLocker sync.RWMutex

	indexTable *indexTable
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
	indexTable  *indexTable
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

func (m *mStreamTable) loadOrCreateMStream(name string) (*mStream, bool) {
	m.locker.Lock()
	ms, ok := m.mStreams[name]
	if ok {
		m.locker.Unlock()
		return ms, true
	}
	size, _ := m.sizeMap.get(name)
	ms = newMStream(size, name)
	m.mStreams[name] = ms
	m.locker.Unlock()
	return ms, false
}

//appendEntry append entry mstream,and return the mStream if it created
func (m *mStreamTable) appendEntry(e *entry) *mStream {
	ms, load := m.loadOrCreateMStream(e.name)
	n := ms.write(e.data)
	m.sizeMap.set(e.name, n)
	m.mSize += int64(len(e.data))
	m.lastEntryID = e.ID
	if load {
		return nil
	}
	return ms
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
	if len(c.immutableMStreamMaps) > c.maxImmutableMStreamMapSize {
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

func (c *committer) start() {
	go func() {
		for {
			entries := c.queue.take()
			for i := range entries {
				mStream := c.mutableMStreamMap.appendEntry(entries[i])
				if mStream != nil {
					c.indexTable.commit(func() {
						c.indexTable.update(mStream)
					})
				}
				if c.mutableMStreamMap.mSize >= c.maxMStreamTableSize {
					c.flush()
				}
			}
		}
	}()
}
