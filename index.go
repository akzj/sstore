package sstore

import (
	"sort"
	"sync"
)

type offsetItem struct {
	segment *segment
	mStream *mStream
	begin   int64
	end     int64
}

type offsetIndex struct {
	name  string
	l     sync.RWMutex
	items []offsetItem
}

var offsetIndexNoFind = offsetItem{}

func newOffsetIndex(name string) *offsetIndex {
	return &offsetIndex{
		name:  name,
		items: nil,
		l:     sync.RWMutex{},
	}
}

func (index *offsetIndex) find(offset int64) (offsetItem, error) {
	index.l.RLock()
	defer index.l.RUnlock()
	if len(index.items) == 0 {
		return offsetIndexNoFind, errNoFindOffsetIndex
	}
	if index.items[len(index.items)-1].begin <= offset {
		return index.items[len(index.items)-1], nil
	}
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin >= offset
	})
	if i < len(index.items) {
		return index.items[i], nil
	}
	return offsetIndexNoFind, errNoFindOffsetIndex
}

func (index *offsetIndex) update(item offsetItem) {
	index.l.Lock()
	defer index.l.Unlock()
	if len(index.items) == 0 {
		index.items = append(index.items, item)
		return
	}
	if index.items[len(index.items)-1].begin < item.begin {
		index.items[len(index.items)-1].end = item.begin
		index.items = append(index.items, item)
		return
	}
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin >= item.begin
	})
	if i < len(index.items) && index.items[i].begin == item.begin {
		index.items[i].end = item.end
		if item.segment != nil {
			if index.items[i].segment != nil {
				panic("segment not nil")
			}
			index.items[i].segment = item.segment
		}
		if item.mStream != nil {
			index.items[i].mStream = item.mStream
		}
	} else {
		panic("index update error")
	}
}

func (index *offsetIndex) remove(item offsetItem) {
	index.l.Lock()
	defer index.l.Unlock()
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin >= item.begin
	})
	if i < len(index.items) && index.items[i].begin == item.begin {
		if item.segment != nil {
			if index.items[i].segment == nil {
				panic("segment null")
			}
			index.items[i].segment = nil
		}
		if item.mStream != nil {
			index.items[i].mStream = nil
		}
		if index.items[i].mStream == nil && index.items[i].segment == nil {
			copy(index.items[i:], index.items[i+1:])
			index.items[len(index.items)-1] = offsetItem{}
			index.items = index.items[:len(index.items)-1]
		}
	} else {
		panic("index remove error")
	}
}

type indexTable struct {
	l        sync.RWMutex
	indexMap map[string]*offsetIndex

	commitAction chan func()
}

func newIndexTable() *indexTable {
	return &indexTable{
		l:            sync.RWMutex{},
		indexMap:     map[string]*offsetIndex{},
		commitAction: make(chan func(), 1),
	}
}

func (index *indexTable) startCommitRoutine() {
	go func() {
		for {
			select {
			case f := <-index.commitAction:
				f()
			}
		}
	}()
}

func (index *indexTable) commit(f func()) {
	index.commitAction <- f
}

func (index *indexTable) loadOrCreate(name string) *offsetIndex {
	index.l.Lock()
	defer index.l.Unlock()
	if offsetIndex, ok := index.indexMap[name]; ok {
		return offsetIndex
	}
	offsetIndex := newOffsetIndex(name)
	index.indexMap[name] = offsetIndex
	return offsetIndex
}

func (index *indexTable) get(name string) *offsetIndex {
	index.l.Lock()
	defer index.l.Unlock()
	if offsetIndex, ok := index.indexMap[name]; ok {
		return offsetIndex
	}
	return nil
}

func (index *indexTable) update1(segment *segment) {
	for _, it := range segment.header.Indexes {
		segment.refInc()
		index.loadOrCreate(it.Name).update(offsetItem{
			segment: segment,
			mStream: nil,
			begin:   it.Begin,
			end:     it.Begin + it.End,
		})
	}
}

func (index *indexTable) remove1(segment *segment) {
	for _, it := range segment.header.Indexes {
		if offsetIndex := index.get(it.Name); offsetIndex != nil {
			offsetIndex.remove(offsetItem{
				segment: segment,
				mStream: nil,
				begin:   it.Begin,
				end:     it.Begin + it.End,
			})
			segment.refDec()
		} else {
			panic("no find offsetIndex")
		}
	}
}

func (index *indexTable) update(stream *mStream) {
	index.loadOrCreate(stream.name).update(offsetItem{
		segment: nil,
		mStream: stream,
		begin:   stream.begin,
		end:     stream.end,
	})
}

func (index *indexTable) remove(stream *mStream) {
	if offsetIndex := index.get(stream.name); offsetIndex != nil {
		offsetIndex.remove(offsetItem{
			segment: nil,
			mStream: stream,
			begin:   stream.begin,
			end:     stream.end,
		})
	} else {
		panic("no find offsetIndex")
	}
}

func (index *indexTable) reader(name string) *reader {
	offsetIndex := index.get(name)
	if offsetIndex == nil {
		return nil
	}
	return newReader(name, 0, offsetIndex)
}