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
	"sort"
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
	filesSnapshotType = "filesSnapshot"
	segmentExt        = ".seg"
	filesWalExt       = ".files.log"
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
	inRecovery    bool

	EntryID      int64    `json:"entry_id"`
	SegmentFiles []string `json:"segment_files"`
	WalFiles     []string `json:"wal_files"`

	notifyS chan interface{}
}

func openFiles(filesDir string, segmentDir string) *files {
	return &files{
		maxWalSize:    128 * 1024 * 1024,
		l:             sync.RWMutex{},
		segmentDir:    segmentDir,
		filesDir:      filesDir,
		segmentIndex:  0,
		walIndex:      0,
		filesWalIndex: 0,
		inRecovery:    false,
		EntryID:       0,
		SegmentFiles:  nil,
		WalFiles:      nil,
		notifyS:       make(chan interface{}),
	}
}
func copyStrings(strings []string) []string {
	return append(make([]string, 0, len(strings)), strings...)
}
func (f *files) getSegmentFiles() []string {
	f.l.RLock()
	defer f.l.RUnlock()
	return copyStrings(f.SegmentFiles)
}

func (f *files) getWalFiles() []string {
	f.l.RLock()
	defer f.l.RUnlock()
	return copyStrings(f.WalFiles)
}

func (f *files) recovery() error {
	f.inRecovery = true
	defer func() {
		f.inRecovery = false
	}()
	var walFiles []string
	err := filepath.Walk(f.filesDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), filesWalExtTmp) {
			_ = os.Remove(path)
		}
		if strings.HasSuffix(info.Name(), filesWalExt) {
			walFiles = append(walFiles, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(walFiles) == 0 {
		f.filesWalIndex = 1
		f.wal, err = openWal(filepath.Join(f.filesDir, "1"+filesWalExt))
	} else {
		sortIntFilename(walFiles)
		f.wal, err = openWal(walFiles[len(walFiles)-1])
		if err != nil {
			return err
		}
	}

	err = f.wal.read(func(e *entry) error {
		f.EntryID = e.ID
		switch e.name {
		case appendSegmentType:
			var request appendSegment
			if err := json.Unmarshal(e.data, &request); err != nil {
				return err
			}
			return f.appendSegment(request)
		case filesSnapshotType:
			if err := json.Unmarshal(e.data, f); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(f.SegmentFiles) > 0 {
		sortIntFilename(f.SegmentFiles)
		f.segmentIndex, err = parseFilenameIndex(f.SegmentFiles[len(f.SegmentFiles)-1])
		if err != nil {
			return err
		}
	}
	if len(walFiles) > 0 {
		for _, filename := range walFiles[:len(walFiles)-1] {
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
		f.filesWalIndex, err = parseFilenameIndex(walFiles[len(walFiles)-1])
		if err != nil {
			return err
		}
	}
	return nil
}

var _files *files

func (f *files) getNextSegment() string {
	atomic.AddInt64(&f.segmentIndex, 1)
	return filepath.Join(f.segmentDir, strconv.FormatInt(f.segmentIndex, 10)+segmentExt)
}

func (f *files) makeSnapshot() {
	f.l.Lock()
	defer f.l.Unlock()
	if f.wal.fileSize() < f.maxWalSize {
		return
	}
	f.filesWalIndex++
	f.EntryID++
	tmpWal := strconv.FormatInt(f.filesWalIndex, 10) + filesWalExtTmp
	tmpWal = filepath.Join(f.filesDir, tmpWal)
	wal, err := createWal(tmpWal)
	if err != nil {
		log.Fatal(err.Error())
	}
	data, err := json.Marshal(f)
	if err != nil {
		log.Fatal(err.Error())
	}
	if err := wal.write(&entry{
		ID:   f.EntryID,
		name: filesSnapshotType,
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

func (f *files) appendSegment(appendS appendSegment) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(appendS.Filename)
	for _, file := range f.SegmentFiles {
		if file == filename {
			return fmt.Errorf("filename repeated")
		}
	}
	f.SegmentFiles = append(f.SegmentFiles, filename)
	if f.inRecovery {
		return nil
	}
	f.EntryID++
	data, _ := json.Marshal(appendS)
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

func parseFilenameIndex(filename string) (int64, error) {
	filename = filepath.Base(filename)
	token := strings.SplitN(filename, ".", 2)[0]
	return strconv.ParseInt(token, 10, 64)
}

func sortIntFilename(intFiles []string) {
	sort.Slice(intFiles, func(i, j int) bool {
		iLen := len(intFiles[i])
		jLen := len(intFiles[j])
		if iLen != jLen {
			return iLen < jLen
		}
		return intFiles[i] < intFiles[j]
	})
}
