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
	"path/filepath"
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
	files       *files

	blockSize int64

	callbackWorker *callbackWorker
	callbackQueue  *entryQueue
}

func newCommitter(options Options,
	endWatchers *endWatchers,
	indexTable *indexTable,
	segments map[string]*segment,
	sizeMap *int64LockMap,
	mutableMStreamMap *mStreamTable,
	queue *entryQueue,
	files *files,
	blockSize int64) *committer {

	callbackQueue := newEntryQueue(128)

	return &committer{
		files:                         files,
		queue:                         queue,
		blockSize:                     blockSize,
		maxMStreamTableSize:           options.MaxMStreamTableSize,
		mutableMStreamMap:             mutableMStreamMap,
		sizeMap:                       sizeMap,
		immutableMStreamMaps:          make([]*mStreamTable, 0, 32),
		locker:                        new(sync.RWMutex),
		flusher:                       newFlusher(files),
		segments:                      segments,
		segmentsLocker:                new(sync.RWMutex),
		indexTable:                    indexTable,
		endWatchers:                   endWatchers,
		maxImmutableMStreamTableCount: options.MaxImmutableMStreamTableCount,
		callbackWorker:                newCallbackWorker(callbackQueue),
		callbackQueue:                 callbackQueue,
	}
}

func (c *committer) appendSegment(filename string, segment *segment) {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment.refInc()
	c.segments[filepath.Base(filename)] = segment
	c.indexTable.update1(segment)
}

func (c *committer) getSegment(filename string) *segment {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment, _ := c.segments[filename]
	return segment
}

func (c *committer) deleteSegment(filename string) error {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment, ok := c.segments[filename]
	if ok == false {
		return errNoFindSegment
	}
	delete(c.segments, filename)
	if err := c.indexTable.remove1(segment); err != nil {
		return err
	}
	segment.refDec()
	return nil
}

func (c *committer) flushCallback(filename string, table *mStreamTable) {
	segment, err := openSegment(filename)
	if err != nil {
		log.Fatal(err.Error())
	}
	var remove = false
	c.locker.Lock()
	if c.immutableMStreamMaps[0] != table {
		panic("flushCallback error")
	}
	if len(c.immutableMStreamMaps) > c.maxImmutableMStreamTableCount {
		copy(c.immutableMStreamMaps[0:], c.immutableMStreamMaps[1:])
		c.immutableMStreamMaps[len(c.immutableMStreamMaps)-1] = nil
		c.immutableMStreamMaps = c.immutableMStreamMaps[:len(c.immutableMStreamMaps)-1]
		remove = true
	}
	c.locker.Unlock()

	c.appendSegment(filename, segment)
	if remove {
		//update indexTable
		for _, mStream := range table.mStreams {
			c.indexTable.remove(mStream)
		}
	}
	if err := c.files.appendSegment(appendSegment{Filename: filename}); err != nil {
		log.Fatal(err.Error())
	}
}

func (c *committer) flush() {
	mStreamMap := c.mutableMStreamMap
	c.mutableMStreamMap = newMStreamTable(c.sizeMap, c.blockSize,
		len(c.mutableMStreamMap.mStreams))
	c.locker.Lock()
	c.immutableMStreamMaps = append(c.immutableMStreamMaps, mStreamMap)
	c.locker.Unlock()
	c.flusher.append(mStreamMap, func(filename string, err error) {
		if err != nil {
			log.Fatal(err.Error())
		}
		c.flushCallback(filename, mStreamMap)
	})
}

func (c *committer) start() {
	c.callbackWorker.start()
	go func() {
		for {
			entries := c.queue.take()
			for i := range entries {
				entry := entries[i]
				mStream, end := c.mutableMStreamMap.appendEntry(entry)
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
			c.callbackQueue.putEntries(entries)
		}
	}()
}

type callbackWorker struct {
	queue *entryQueue
}

func newCallbackWorker(queue *entryQueue) *callbackWorker {
	return &callbackWorker{queue: queue}
}

func (worker *callbackWorker) start() {
	go func() {
		for {
			entries := worker.queue.take()
			for index := range entries {
				e := entries[index]
				e.cb(nil)
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
	blockSize   int64
}

func newMStreamTable(sizeMap *int64LockMap,
	blockSize int64, mStreamMapSize int) *mStreamTable {
	return &mStreamTable{
		blockSize:   blockSize,
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
	ms = newMStream(size, m.blockSize, name)
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
