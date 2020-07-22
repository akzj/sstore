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
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type appendWal struct {
	Filename string `json:"filename"`
}
type deleteWal struct {
	Filename string `json:"filename"`
}

type appendSegment struct {
	Filename string `json:"filename"`
}

type deleteSegment struct {
	Filename string `json:"filename"`
}

type delWalHeader struct {
	Filename string `json:"filename"`
}

type files struct {
	maxWalSize    int64
	wal           *wal
	l             sync.RWMutex
	segmentDir    string
	filesDir      string
	walDir        string
	segmentIndex  int64
	walIndex      int64
	filesWalIndex int64
	inRecovery    bool

	EntryID      int64                `json:"entry_id"`
	SegmentFiles []string             `json:"segment_files"`
	WalFiles     []string             `json:"wal_files"`
	WalHeaderMap map[string]walHeader `json:"wal_header_map"`

	notifyS chan interface{}
}

const (
	appendSegmentType = "appendSegment" //append segment
	deleteSegmentType = "deleteSegment"
	appendWalType     = "appendWal"
	deleteWalType     = "deleteWal"
	setWalHeaderType  = "setWalHeader" //set wal header
	delWalHeaderType  = "delWalHeader" //set wal header

	filesSnapshotType = "filesSnapshot"

	segmentExt     = ".seg"
	WalExt         = ".log"
	filesWalExt    = ".files"
	filesWalExtTmp = ".files.tmp"
)

func openFiles(filesDir string, segmentDir string, walDir string) (*files, error) {
	files := &files{
		maxWalSize:    128 * 1024 * 1024,
		l:             sync.RWMutex{},
		segmentDir:    segmentDir,
		filesDir:      filesDir,
		walDir:        walDir,
		segmentIndex:  0,
		walIndex:      0,
		filesWalIndex: 0,
		inRecovery:    false,
		EntryID:       0,
		SegmentFiles:  nil,
		WalFiles:      nil,
		notifyS:       make(chan interface{}),
	}
	if err := files.recovery(); err != nil {
		return nil, err
	}
	return files, nil
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
			return errors.WithStack(err)
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
		return errors.WithStack(err)
	}

	sortIntFilename(walFiles)
	if len(walFiles) == 0 {
		f.filesWalIndex = 1
		f.wal, err = openWal(filepath.Join(f.filesDir, "1"+filesWalExt))
	} else {
		f.wal, err = openWal(walFiles[len(walFiles)-1])
		if err != nil {
			return err
		}
	}
	if err := f.wal.seekStart(); err != nil {
		return err
	}
	err = f.wal.read(func(e *entry) error {
		f.EntryID = e.ID
		fmt.Println(e.ID, string(e.data), e.name)
		switch e.name {
		case appendSegmentType:
			var appendS appendSegment
			if err := json.Unmarshal(e.data, &appendS); err != nil {
				return errors.WithStack(err)
			}
			return f.appendSegment(appendS)
		case deleteSegmentType:
			var deleteS deleteSegment
			if err := json.Unmarshal(e.data, &deleteS); err != nil {
				return errors.WithStack(err)
			}
			return f.deleteSegment(deleteS)
		case appendWalType:
			var appendW appendWal
			if err := json.Unmarshal(e.data, &appendW); err != nil {
				return errors.WithStack(err)
			}
			return f.appendWal(appendW)
		case deleteWalType:
			var deleteW deleteWal
			if err := json.Unmarshal(e.data, &deleteW); err != nil {
				return errors.WithStack(err)
			}
			return f.deleteWal(deleteW)
		case filesSnapshotType:
			if err := json.Unmarshal(e.data, f); err != nil {
				return errors.WithStack(err)
			}
		case setWalHeaderType:
			var header walHeader
			if err := json.Unmarshal(e.data, &header); err != nil {
				return errors.WithStack(err)
			}
			return f.setWalHeader(header)
		default:
			log.Fatalf("unknown type %s", e.name)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if len(f.SegmentFiles) > 0 {
		sortIntFilename(f.SegmentFiles)
		f.segmentIndex, err = parseFilenameIndex(f.SegmentFiles[len(f.SegmentFiles)-1])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if len(walFiles) > 0 {
		for _, filename := range walFiles[:len(walFiles)-1] {
			if err := os.Remove(filename); err != nil {
				return errors.WithStack(err)
			}
		}
		f.filesWalIndex, err = parseFilenameIndex(walFiles[len(walFiles)-1])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

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
	wal, err := openWal(tmpWal)
	if err != nil {
		log.Fatal(err)
	}
	data, err := json.Marshal(f)
	if err != nil {
		log.Fatal(err)
	}
	if err := wal.write(&entry{
		ID:   f.EntryID,
		name: filesSnapshotType,
		data: data,
		cb:   nil,
	}); err != nil {
		log.Fatal(err)
	}
	if err := wal.flush(); err != nil {
		log.Fatal(err)
	}
	if err := wal.close(); err != nil {
		log.Fatal(err)
	}
	filename := strings.ReplaceAll(tmpWal, filesWalExtTmp, filesWalExt)
	if err := os.Rename(tmpWal, filename); err != nil {
		log.Fatal(err)
	}
	if err := f.wal.flush(); err != nil {
		log.Fatal(err)
	}
	if err := f.wal.close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(f.wal.Filename()); err != nil {
		log.Fatal(err)
	}
	f.wal, err = openWal(filename)
	if err != nil {
		log.Fatal(err)
	}
}

func (f *files) appendWal(appendW appendWal) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(appendW.Filename)
	for _, file := range f.WalFiles {
		if file == filename {
			return errors.Errorf("wal filename repeated")
		}
	}
	f.WalFiles = append(f.WalFiles, filename)
	sortIntFilename(f.WalFiles)
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(appendW)
	return f.writeEntry(appendWalType, data)
}

func (f *files) deleteWal(deleteW deleteWal) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(deleteW.Filename)
	var find = false
	for index, file := range f.WalFiles {
		if file == filename {
			copy(f.WalFiles[index:], f.WalFiles[index+1:])
			f.WalFiles = f.WalFiles[:len(f.WalFiles)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find wal file [%s]", filename)
	}
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(deleteW)
	return f.writeEntry(deleteWalType, data)
}

func (f *files) appendSegment(appendS appendSegment) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(appendS.Filename)
	for _, file := range f.SegmentFiles {
		if file == filename {
			return fmt.Errorf("segment filename repeated")
		}
	}
	f.SegmentFiles = append(f.SegmentFiles, filename)
	sortIntFilename(f.SegmentFiles)
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(appendS)
	return f.writeEntry(appendSegmentType, data)
}

func (f *files) setWalHeader(header walHeader) error {
	f.l.Lock()
	defer f.l.Unlock()
	f.WalHeaderMap[filepath.Base(header.Filename)] = header
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(header)
	return f.writeEntry(setWalHeaderType, data)
}

func (f *files) getWalHeader(filename string) (walHeader, error) {
	f.l.Lock()
	defer f.l.Unlock()
	header, ok := f.WalHeaderMap[filename]
	if !ok {
		return header, errors.Errorf("no find header [%s]", filename)
	}
	return header, nil
}

func (f *files) delWalHeader(header delWalHeader) error {
	f.l.Lock()
	defer f.l.Unlock()
	_, ok := f.WalHeaderMap[filepath.Base(header.Filename)]
	if ok == false {
		return errors.Errorf("no find wal [%s]", header.Filename)
	}
	delete(f.WalHeaderMap, filepath.Base(header.Filename))
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(header)
	return f.writeEntry(delWalHeaderType, data)
}

func (f *files) deleteSegment(deleteS deleteSegment) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(deleteS.Filename)
	var find = false
	for index, file := range f.SegmentFiles {
		if file == filename {
			copy(f.SegmentFiles[index:], f.SegmentFiles[index+1:])
			f.SegmentFiles = f.SegmentFiles[:len(f.SegmentFiles)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find segment [%s]", filename)
	}
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(deleteS)
	return f.writeEntry(deleteSegmentType, data)
}

func (f *files) writeEntry(typ string, data []byte, ) error {
	f.EntryID++
	if err := f.wal.write(&entry{
		ID:   f.EntryID,
		name: typ,
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

func (f *files) getNextWal() string {
	f.l.Lock()
	f.l.Unlock()
	f.walIndex++
	return filepath.Join(f.walDir, strconv.FormatInt(f.walIndex, 10)+WalExt)
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
