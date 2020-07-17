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
	"io"
	"os"
	"time"
)

type indexInfo struct {
	Name  string
	Base  int64
	Begin int64
	Size  int64
	CRC   int64
}
type segmentHeader struct {
	GcTS        time.Time            `json:"gc_ts"`
	Size        int64                `json:"size"`
	LastEntryID int64                `json:"last_entry_id"`
	Indexes     map[string]indexInfo `json:"indexes"`
}

type segment struct {
	filename string
	header   *segmentHeader
	f        *os.File
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
