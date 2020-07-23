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
	"sync"
	"time"
)

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
