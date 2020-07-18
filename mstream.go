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
	"io"
	"sync"
)

//memory stream
type mStream struct {
	locker sync.RWMutex
	name   string
	base   int64
	size   int64
	blocks []block
}

func newMStream(begin int64, name string) *mStream {
	return &mStream{
		name:   name,
		base:   begin,
		size:   0,
		blocks: nil,
	}
}

func (m *mStream) ReadAt(p []byte, off int64) (n int, err error) {
	panic("implement me")
}

func (m *mStream) Write(p []byte) int64 {
	return m.base + m.size
}

func (m *mStream) writeTo(writer io.Writer) (int, error) {
	m.locker.RLock()
	defer m.locker.RUnlock()
	var n int
	for i := range m.blocks {
		ret, err := (&m.blocks[i]).writeTo(writer)
		n += ret
		if err != nil {
			return n, err
		}
	}
	return n,nil
}

const blockSize = 4 * 1024

type block struct {
	begin int64
	buf   []byte
	pos   int
	limit int
}

func (block *block) writeTo(writer io.Writer) (int, error) {
	return writer.Write(block.buf[:block.pos])
}
