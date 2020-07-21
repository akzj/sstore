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

	fileS := openFiles(sStore.options.FilesDir, sStore.options.SegmentDir)
	sStore.files = fileS

	segmentFiles := fileS.getSegmentFiles()
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(sStore.options.SegmentDir, file))
		if err != nil {
			return errors.WithStack(err)
		}
		for _, index := range segment.header.Indexes {
			sStore.endMap.set(index.Name, index.End)
		}
		sStore.entryID = segment.header.LastEntryID
		sStore.segments[file] = segment
		sStore.indexTable.update1(segment)
	}

	walFiles := fileS.getWalFiles()
	for _, file := range walFiles {
		wal, err := openWal(filepath.Join(sStore.options.WalDir, file))
		if err != nil {
			return err
		}
		if err := wal.read(func(e *entry) error {
			if e.ID <= sStore.entryID {
				return nil
			} else if e.ID == sStore.entryID+1 {
				sStore.committer.queue.put(e)
				return nil
			} else {
				return errors.WithStack(ErrWal)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}
