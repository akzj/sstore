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
	"encoding/binary"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"os"
)

const version1 = "ver1"
const maxHeaderSize = 1024

type walHeader struct {
	Version      string `json:"V"`
	FirstEntryID int64  `json:"F"`
	LastEntryID  int64  `json:"L"`
	Old          bool   `json:"O"`
}

// write ahead log
type wal struct {
	filename string
	size     int64
	f        *os.File
	buffer   *bufio.Writer
	header   walHeader
}

func openWal(filename string) (*wal, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w := &wal{
		filename: filename,
		size:     0,
		f:        f,
		buffer:   bufio.NewWriterSize(f, 4*1024*1024),
		header: walHeader{
			Version:      version1,
			FirstEntryID: -1,
			LastEntryID:  -1,
		},
	}
	var dataLen int32
	if err := binary.Read(f, binary.BigEndian, &dataLen); err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if dataLen > maxHeaderSize {
		return nil, errors.Errorf("headerSize %d > %d", dataLen, maxHeaderSize)
	}
	data := make([]byte, dataLen)
	if _, err := f.Read(data); err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if err := json.Unmarshal(data, &w.header); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := w.seekStart(); err != nil {
		return nil, err
	}
	return w, nil
}

func createWal(filename string) (*wal, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w := &wal{
		filename: filename,
		size:     0,
		f:        f,
		buffer:   bufio.NewWriterSize(f, 4*1024*1024),
		header: walHeader{
			Version:      version1,
			FirstEntryID: -1,
			LastEntryID:  -1,
			Old:          false,
		},
	}
	if err := w.flushHeader(false); err != nil {
		_ = f.Close()
		_ = os.Remove(filename)
		return nil, err
	}
	if err := w.seekStart(); err != nil {
		_ = f.Close()
		_ = os.Remove(filename)
		return nil, err
	}
	return w, nil
}

func (wal *wal) seekStart() error {
	if _, err := wal.f.Seek(maxHeaderSize, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) seekEnd() error {
	if _, err := wal.f.Seek(0, io.SeekEnd); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) flushHeader(old bool) error {
	wal.header.Old = old
	data, _ := json.Marshal(wal.header)
	if _, err := wal.f.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(wal.f, binary.BigEndian, int32(len(data))); err != nil {
		return errors.WithStack(err)
	}
	if _, err := wal.f.Write(data); err != nil {
		return errors.WithStack(err)
	}
	if _, err := wal.f.Seek(0, io.SeekEnd); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) getHeader() walHeader {
	return wal.header
}

func (wal *wal) flush() error {
	if err := wal.buffer.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) sync() error {
	if err := wal.f.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) write(e *entry) error {
	wal.header.LastEntryID = e.ID
	if wal.header.FirstEntryID == -1 {
		wal.header.FirstEntryID = e.ID
	}
	if err := e.write(wal.buffer); err != nil {
		return err
	}
	wal.size += int64(e.size())
	return nil
}

func (wal *wal) fileSize() int64 {
	return wal.size
}

func (wal *wal) close() error {
	if err := wal.flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := wal.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wal *wal) Filename() string {
	return wal.filename
}

func (wal *wal) read(cb func(e *entry) error) error {
	reader := bufio.NewReader(wal.f)
	for {
		e, err := decodeEntry(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.WithStack(err)
		}
		if err := cb(e); err != nil {
			return err
		}
	}
}
