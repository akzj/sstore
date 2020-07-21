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
	"path/filepath"
)

//recover segment,wal,index
func recover(sStore *SStore) error {

	files := openFiles(sStore.options.FilesDir,
		sStore.options.SegmentDir,
		sStore.options.WalDir)
	sStore.files = files

	segmentFiles := files.getSegmentFiles()
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(sStore.options.SegmentDir, file))
		if err != nil {
			return errors.WithStack(err)
		}
		for _, info := range segment.meta.OffSetInfos {
			sStore.endMap.set(info.Name, info.End)
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
		if err := wal.read(func(e *entry) error {
			if e.ID <= sStore.entryID {
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
			return errors.WithStack(err)
		}
	}
	var w *wal
	var err error
	if len(walFiles) > 0 {
		last := walFiles[len(walFiles)-1]
		w, err = openWal(filepath.Join(sStore.options.WalDir, last))
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		file := files.getNextWal()
		w, err = createWal(file)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	sStore.wWriter = newWWriter(w, sStore.entryQueue, sStore.committer.queue)
	return nil
}
