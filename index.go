package sstore

import (
	"sort"
	"sync"
)

type offsetIndex struct {
	segment *segment
	mStream *mStream
	begin   int64
	end     int64
}

type Index struct {
	l               sync.RWMutex
	lastOffsetIndex offsetIndex

	offsetIndices []offsetIndex
}

var offsetIndexNoFind = offsetIndex{}

func (index *Index) findOffsetIndex(offset int64) (offsetIndex, error) {
	index.l.RLock()
	defer index.l.RUnlock()
	if index.lastOffsetIndex.begin <= offset {
		return index.lastOffsetIndex, nil
	}
	i := sort.Search(len(index.offsetIndices), func(i int) bool {
		return index.offsetIndices[i].begin >= offset
	})
	if i < len(index.offsetIndices) {
		return index.offsetIndices[i], nil
	}
	return offsetIndexNoFind, errNoFindOffsetIndex
}
