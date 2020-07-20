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
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

type indexInfo struct {
	Name   string `json:"name"`
	Begin  int64  `json:"begin"`
	Offset int64  `json:"offset"`
	End    int64  `json:"end"`
	CRC    uint32 `json:"crc"`
}

type segmentHeader struct {
	GcTS        time.Time            `json:"gc_ts"`
	LastEntryID int64                `json:"last_entry_id"`
	Indexes     map[string]indexInfo `json:"indexes"`
}

type segment struct {
	*ref
	filename string
	header   *segmentHeader
	f        *os.File
	l        sync.RWMutex
}

func createSegment(filename string) (*segment, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &segment{
		filename: filename,
		header:   new(segmentHeader),
		f:        f,
	}, nil
}

func openSegment(filename string) (*segment, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	segment := &segment{
		filename: filename,
		header:   new(segmentHeader),
		f:        f,
	}
	segment.ref = newRef(0, func() {
		if err := segment.close(); err != nil {
			log.Fatal(err.Error())
		}
	})
	return segment, nil
}

func (s *segment) indexInfo(name string) (indexInfo, error) {
	indexInfo, ok := s.header.Indexes[name]
	if ok == false {
		return indexInfo, errNoFindIndexInfo
	}
	return indexInfo, nil
}

func (s *segment) Reader(name string) *segmentReader {
	info, ok := s.header.Indexes[name]
	if ok {
		return nil
	}
	return &segmentReader{
		indexInfo: info,
		r:         io.NewSectionReader(s.f, info.Offset, info.End),
	}
}

func (s *segment) flushMStreamTable(table *mStreamTable) error {
	s.l.Lock()
	defer s.l.Unlock()
	var Offset int64
	writer := bufio.NewWriterSize(s.f, 1024*1024)
	for name, mStream := range table.mStreams {
		hash := crc32.NewIEEE()
		mWriter := io.MultiWriter(writer, hash)
		n, err := mStream.writeTo(mWriter)
		if err != nil {
			return err
		}
		index := indexInfo{
			Name:   name,
			Offset: Offset,
			End:    int64(n),
			CRC:    hash.Sum32(),
			Begin:  mStream.begin,
		}
		Offset += int64(n)
		s.header.Indexes[name] = index
	}
	s.header.LastEntryID = table.lastEntryID
	s.header.GcTS = table.GcTS
	data, _ := json.Marshal(s.header)
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *segment) close() error {
	s.l.Lock()
	defer s.l.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	s.f = nil
	return nil
}

type segmentReader struct {
	indexInfo indexInfo
	r         *io.SectionReader
}

func (s *segmentReader) Seek(offset int64, whence int) (int64, error) {
	if offset < s.indexInfo.Begin || offset >= s.indexInfo.End {
		return 0, errOffSet
	}
	offset = offset - s.indexInfo.Begin
	return s.r.Seek(offset, whence)
}

func (s *segmentReader) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < s.indexInfo.Begin || offset >= s.indexInfo.End {
		return 0, errOffSet
	}
	offset = offset - s.indexInfo.Begin
	return s.r.ReadAt(p, offset)
}

func (s *segmentReader) Read(p []byte) (n int, err error) {
	return s.r.Read(p)
}

type ref struct {
	l sync.RWMutex
	c int32
	f func()
}

func newRef(count int32, f func()) *ref {
	return &ref{
		c: count,
		f: f,
	}
}

func (ref *ref) refCount() int32 {
	ref.l.Lock()
	c := ref.c
	ref.l.Unlock()
	return c
}

func (ref *ref) refDec() int32 {
	ref.l.Lock()
	if ref.c <= 0 {
		panic(fmt.Errorf("ref.c %d error", ref.c))
	}
	ref.c -= 1
	if ref.c == 0 {
		ref.c = math.MinInt32
		ref.l.Unlock()
		go ref.f()
		return 0
	}
	return ref.c
}

func (ref *ref) refInc() int32 {
	ref.l.Lock()
	defer ref.l.Unlock()
	ref.c += 1
	return ref.c
}
