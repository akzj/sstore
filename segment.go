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
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

type indexInfo struct {
	Name   string
	Base   int64
	Offset int64
	Size   int64
	CRC    uint32
}

type segmentHeader struct {
	GcTS        time.Time            `json:"gc_ts"`
	LastEntryID int64                `json:"last_entry_id"`
	Indexes     map[string]indexInfo `json:"indexes"`
}

type segment struct {
	filename string
	header   *segmentHeader
	f        *os.File
	l        sync.RWMutex
}

func createSegment(filename string) *segment {
	f, err := os.Create(filename)
	if err != nil {
		return nil
	}
	return &segment{
		filename: filename,
		header:   new(segmentHeader),
		f:        f,
	}
}

func openSegment(filename string) *segment {
	return nil
}

func (s *segment) indexInfo(name string) (indexInfo, error) {
	indexInfo, ok := s.header.Indexes[name]
	if ok == false {
		return indexInfo, errNoFindIndexInfo
	}
	return indexInfo, nil
}

func (s *segment) Reader(name string) *io.SectionReader {
	info, ok := s.header.Indexes[name]
	if ok {
		return nil
	}
	return io.NewSectionReader(s.f, info.Base, info.Size)
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
			Size:   int64(n),
			CRC:    hash.Sum32(),
			Base:   mStream.base,
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
