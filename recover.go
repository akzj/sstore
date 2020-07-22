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
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
)

func mkdir(dir string) error {
	f, err := os.Open(dir)
	if err == nil {
		_ = f.Close()
		return nil
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

//recover segment,wal,index
func recover(sStore *SStore) error {
	for _, dir := range []string{
		sStore.options.WalDir,
		sStore.options.FilesDir,
		sStore.options.SegmentDir} {
		if err := mkdir(dir); err != nil {
			return err
		}
	}
	files := openFiles(sStore.options.FilesDir,
		sStore.options.SegmentDir,
		sStore.options.WalDir)
	sStore.files = files

	mStreamTable := newMStreamTable(sStore.endMap, sStore.options.BlockSize, 128)
	commitQueue := newEntryQueue(sStore.options.EntryQueueCap)
	committer := newCommitter(sStore.options,
		sStore.endWatchers,
		sStore.indexTable,
		sStore.segments,
		sStore.endMap,
		mStreamTable,
		commitQueue,
		files,
		sStore.options.BlockSize)
	sStore.committer = committer

	segmentFiles := files.getSegmentFiles()
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(sStore.options.SegmentDir, file))
		if err != nil {
			return errors.WithStack(err)
		}
		for _, info := range segment.meta.OffSetInfos {
			sStore.endMap.set(info.Name, info.End)
		}
		if segment.meta.LastEntryID <= sStore.entryID {
			return errors.Errorf("segment meta LastEntryID[%d] error",
				segment.meta.LastEntryID)
		}
		sStore.entryID = segment.meta.LastEntryID
		sStore.segments[file] = segment
		sStore.indexTable.update1(segment)
	}

	walFiles := files.getWalFiles()
	for _, file := range walFiles {
		wal, err := openWal(filepath.Join(sStore.options.WalDir, file))
		if err != nil {
			return err
		}
		walHeader := wal.getHeader()
		//skip
		if walHeader.Old && walHeader.LastEntryID <= sStore.entryID {
			continue
		}
		if err := wal.read(func(e *entry) error {
			if e.ID <= sStore.entryID {
				//skip
				return nil
			} else if e.ID == sStore.entryID+1 {
				end1, _ := sStore.endMap.get(e.name)
				committer := sStore.committer
				mStream, end2 := committer.mutableMStreamMap.appendEntry(e)
				if mStream != nil {
					committer.indexTable.commit(func() {
						committer.indexTable.update(mStream)
					})
				}
				if committer.mutableMStreamMap.mSize >= committer.maxMStreamTableSize {
					committer.flush()
				}
				if end2 != end1+int64(len(e.data)) {
					return errors.Errorf("endMap error %d %d %d", end2, len(e.data), end2)
				}
				return nil
			} else {
				return errors.WithStack(ErrWal)
			}
		}); err != nil {
			_ = wal.close()
			return err
		}
		if err := wal.close(); err != nil {
			return err
		}
	}
	var w *wal
	var err error
	if len(walFiles) > 0 {
		last := walFiles[len(walFiles)-1]
		w, err = openWal(filepath.Join(sStore.options.WalDir, last))
		if err != nil {
			return err
		}
		//seek to end for append new entry
		if err := w.seekEnd(); err != nil {
			return err
		}
	} else {
		file := files.getNextWal()
		w, err = createWal(file)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	sStore.wWriter = newWWriter(w, sStore.entryQueue,
		sStore.committer.queue, sStore.files, sStore.options.MaxWalSize)

	//clear dead wal
	walFileAll, err := listDir(sStore.options.WalDir, WalExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(walFileAll, walFiles) {
		if err := os.Remove(filepath.Join(sStore.options.WalDir, filename)); err != nil {
			return errors.WithStack(err)
		}
	}

	//clear dead segment files
	segmentAllFiles, err := listDir(sStore.options.SegmentDir, segmentExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(segmentAllFiles, segmentFiles) {
		if err := os.Remove(filepath.Join(sStore.options.SegmentDir, filename)); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func listDir(dir string, ext string) ([]string, error) {
	var files []string
	return files, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		if ext != "" && strings.HasSuffix(info.Name(), ext) {
			files = append(files, info.Name())
		}
		return nil
	})
}
func diffStrings(first []string, second []string) []string {
	var diff []string
Loop:
	for _, it := range first {
		for _, ij := range second {
			if it == ij {
				continue Loop
			}
		}
		diff = append(diff, it)
	}
	return diff
}