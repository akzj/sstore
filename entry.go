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
	"encoding/binary"
	"io"
	"sync"
)

type entry struct {
	id   int64
	name string
	data []byte
	cb   func(err error)
}

const (
	maxNameLen = 1024
	maxDataLen = 1024 * 1024 * 128
)

var entriesPool = sync.Pool{New: func() interface{} {
	return make([]entry, 64)
}}

func (e *entry) size() int {
	return len(e.data) + len(e.name) + 16
}

func (e *entry) encode() []byte {
	var buf = make([]byte, e.size())
	writer := bytes.NewBuffer(buf)
	_ = e.write(writer)
	return buf
}

func (e *entry) write(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, uint32(len(e.name))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(e.data))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, e.id); err != nil {
		return err
	}
	if _, err := writer.Write([]byte(e.name)); err != nil {
		return err
	}
	if _, err := writer.Write(e.data); err != nil {
		return err
	}
	return nil
}

func decodeEntry(buf []byte) (*entry, error) {
	var nameLen uint32
	var dataLen uint32
	var e = new(entry)
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &e.id); err != nil {
		return nil, err
	}
	if nameLen > maxNameLen {
		return nil, ErrEntryLengthOfName
	}
	if dataLen > maxDataLen {
		return nil, ErrEntryLengthOfData
	}
	if len(buf) < int(nameLen+dataLen+8) {
		return nil, io.ErrShortBuffer
	}
	var name = make([]byte, nameLen)
	n, _ := reader.Read(name)
	if n != int(nameLen) {
		return nil, ErrEntryLengthOfName
	}
	e.data = make([]byte, dataLen)
	n, _ = reader.Read(e.data)
	if n != int(dataLen) {
		return nil, ErrEntryLengthOfData
	}
	return e, nil
}
