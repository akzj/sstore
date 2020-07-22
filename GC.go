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
)

// GC will delete wal,segment
// delete useless wal
// delete last segment
func (sstore *SStore) gcWal() error {
	walFiles := sstore.files.getWalFiles()
	segmentFiles := sstore.files.getSegmentFiles()
	if len(segmentFiles) == 0 {
		return nil
	}
	last := segmentFiles[len(segmentFiles)-1]
	segment := sstore.committer.getSegment(last)
	if segment == nil {
		return errors.Errorf("no find segment [%d]", last)
	}
	LastEntryID := segment.lastEntryID()
	skip := sstore.wWriter.walFilename()
	for _, filename := range walFiles {
		if filename == skip {
			continue
		}
		walFile := filepath.Join(sstore.options.WalDir, filename)
		wal, err := openWal(walFile)
		if err != nil {
			continue
		}
		walHeader := wal.getHeader()
		if walHeader.Old && walHeader.LastEntryID <= LastEntryID {
			if err := wal.close(); err != nil {
				return err
			}
			if err := sstore.files.deleteWal(deleteWal{Filename: filename}); err != nil {
				return err
			}
			if err := os.Remove(walFile); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (sstore *SStore) gcSegment() error {
	segmentFiles := sstore.files.getSegmentFiles()
	if len(segmentFiles) <= sstore.options.MaxSegmentCount {
		return nil
	}
	var deleteFiles = segmentFiles[:sstore.options.MaxSegmentCount-len(segmentFiles)]
	for _, filename := range deleteFiles {
		segment := sstore.committer.getSegment(filename)
		if segment == nil {
			return errors.Errorf("no find segment[%s]", segment)
		}
		if err := segment.deleteOnClose(true); err != nil {
			return err
		}
		if err := sstore.committer.deleteSegment(filename); err != nil {
			return err
		}
	}
	return nil
}

func (sstore *SStore) GC() error {
	if err := sstore.gcWal(); err != nil {
		return err
	}
	if err := sstore.gcSegment(); err != nil {
		return err
	}
	return nil
}