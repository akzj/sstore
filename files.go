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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type appendSegment struct {
	Filename string `json:"filename"`
}

const (
	appendSegmentType = "appendSegment"
	fileSnapshotType  = "filesSnapshot"
	segmentExt        = ".seg"
	filesWalExt       = ".files.log"
	filesSnapshot     = ".files.snapshot"
	filesWalExtTmp    = ".files.log.tmp"
)

type files struct {
	maxWalSize    int64
	wal           *wal
	l             sync.RWMutex
	segmentDir    string
	filesDir      string
	segmentIndex  int64
	walIndex      int64
	filesWalIndex int64

	EntryID      int64    `json:"entry_id"`
	SegmentFiles []string `json:"segment_files"`
	WalFiles     []string `json:"wal_files"`

	notifyS chan interface{}
}

func openFiles() *files {
	return &files{}
}

func (f *files) recovery() error {
	return f.wal.read(func(e *entry) {
		switch e.name {
		case appendSegmentType:
		case fileSnapshotType:
		}
	})
}

var _files *files

func (f *files) getNextSegment() string {
	atomic.AddInt64(&f.segmentIndex, 1)
	return filepath.Join(f.segmentDir, strconv.FormatInt(f.segmentIndex, 10)+segmentExt)
}

func (f *files) makeSnapshot() {
	f.l.Lock()
	defer f.l.Unlock()
	if f.wal.fileSize() > f.maxWalSize {
		f.filesWalIndex++
		f.EntryID++
		tmpWal := strconv.FormatInt(f.filesWalIndex, 10) + filesWalExtTmp
		tmpWal = filepath.Join(f.filesDir, tmpWal)
		wal, err := createWal(tmpWal)
		if err != nil {
			log.Fatal(err.Error())
		}
		data, _ := json.Marshal(f)
		if err := wal.write(&entry{
			ID:   f.EntryID,
			name: fileSnapshotType,
			data: data,
			cb:   nil,
		}); err != nil {
			log.Fatal(err.Error())
		}
		if err := wal.flush(); err != nil {
			log.Fatal(err.Error())
		}
		if err := wal.close(); err != nil {
			log.Fatal(err.Error())
		}
		filename := strings.ReplaceAll(tmpWal, filesWalExtTmp, filesWalExt)
		if err := os.Rename(tmpWal, filename); err != nil {
			log.Fatal(err.Error())
		}
		if err := f.wal.flush(); err != nil {
			log.Fatal(err.Error())
		}
		if err := f.wal.close(); err != nil {
			log.Fatal(err.Error())
		}
		if err := os.Remove(f.wal.Filename()); err != nil {
			log.Fatal(err.Error())
		}
		f.wal, err = openWal(filename)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func (f *files) appendSegment(filename string) error {
	f.l.Lock()
	defer f.l.Unlock()
	for _, it := range f.SegmentFiles {
		if it == filename {
			return fmt.Errorf("filename repeated")
		}
	}
	f.EntryID++
	f.SegmentFiles = append(f.SegmentFiles, filename)
	data, _ := json.Marshal(appendSegment{Filename: filename})
	if err := f.wal.write(&entry{
		ID:   f.EntryID,
		name: appendSegmentType,
		data: data,
	}); err != nil {
		return err
	}
	if err := f.wal.flush(); err != nil {
		return err
	}
	if f.wal.fileSize() > f.maxWalSize {
		f.notifySnapshot()
	}
	return nil
}

func (f *files) notifySnapshot() {
	select {
	case f.notifyS <- struct{}{}:
	default:
	}
}

func (f *files) start() {
	go func() {
		for {
			select {
			case <-f.notifyS:
				f.makeSnapshot()
			}
		}
	}()
}
