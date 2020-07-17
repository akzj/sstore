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
	"bytes"
	"os"
)

// write ahead log
type wal struct {
	f      *os.File
	bCap   int
	buffer *bytes.Buffer
}

func openWal(filename string) *wal {
	return &wal{}
}

func createWal(filename string) *wal {
	return &wal{}
}

func (wal *wal) flush() error {
	if wal.buffer.Len() > 0 {
		if _, err := wal.f.Write(wal.buffer.Bytes()); err != nil {
			return err
		}
		wal.buffer.Reset()
	}
	return nil
}

func (wal *wal) sync() error {
	return wal.f.Sync()
}

func (wal *wal) write(entry entry) error {
	if wal.buffer.Len() > wal.bCap {
		if err := wal.flush(); err != nil {
			return err
		}
	}
	return entry.write(wal.buffer)
}
