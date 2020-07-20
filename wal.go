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
	"bytes"
	"io"
	"os"
	"sync"
)

const version = "1.0.0"

type walHeader struct {
	Version      string
	Filename     string
	FirstEntryID int64
	LastEntryID  int64
}

// write ahead log
type wal struct {
	filename string
	l        sync.RWMutex
	size     int64
	f        *os.File
	bCap     int
	buffer   *bytes.Buffer
	header   walHeader
}

func openWal(filename string) (*wal, error) {
	return &wal{}, nil
}

func createWal(filename string) (*wal, error) {
	return &wal{}, nil
}

func (wal *wal) getHeader() walHeader {
	return wal.header
}

func (wal *wal) flush() error {
	wal.l.Lock()
	defer wal.l.Unlock()
	if wal.buffer.Len() > 0 {
		if n, err := wal.f.Write(wal.buffer.Bytes()); err != nil {
			return err
		} else {
			wal.size += int64(n)
		}
		wal.buffer.Reset()
	}
	return nil
}

func (wal *wal) sync() error {
	return wal.f.Sync()
}

func (wal *wal) write(e *entry) error {
	wal.l.Lock()
	defer wal.l.Unlock()
	wal.header.LastEntryID = e.ID
	if wal.header.FirstEntryID == -1 {
		wal.header.FirstEntryID = e.ID
	}
	if wal.buffer.Len()+len(e.data) > wal.bCap {
		if err := wal.flush(); err != nil {
			return err
		}
		if len(e.data) > wal.bCap {
			if err := e.write(wal.f); err != nil {
				return err
			}
			wal.size += int64(e.size())
			return nil
		}
	}
	if err := e.write(wal.buffer); err != nil {
		return err
	}
	wal.size += int64(e.size())
	return nil
}

func (wal *wal) fileSize() int64 {
	wal.l.RLock()
	defer wal.l.RUnlock()
	return wal.size
}

func (wal *wal) close() error {
	wal.l.Lock()
	defer wal.l.Unlock()
	return wal.f.Close()
}

func (wal *wal) Filename() string {
	return wal.filename
}

func (wal *wal) read(cb func(e *entry)) error {
	wal.l.RLock()
	defer wal.l.RUnlock()
	reader := bufio.NewReader(wal.f)
	for {
		e, err := decodeEntry(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		cb(e)
	}
}
