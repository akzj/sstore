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
	"io"
	"math"
	"sync"
)

//memory stream
type mStream struct {
	locker    sync.RWMutex
	name      string
	begin     int64
	end       int64
	blocks    []block
	blockSize int
}

const mStreamEnd = math.MaxInt64

func newMStream(begin int64, blockSize int, name string) *mStream {
	blocks := make([]block, 0, 128)
	blocks = append(blocks, makeBlock(begin, blockSize))
	return &mStream{
		locker:    sync.RWMutex{},
		name:      name,
		begin:     begin,
		end:       begin,
		blocks:    blocks,
		blockSize: blockSize,
	}
}

func (m *mStream) ReadAt(p []byte, off int64) (n int, err error) {
	m.locker.RLock()
	defer m.locker.RUnlock()
	if off < m.begin || off > m.end {
		return 0, errors.Wrapf(errOffSet,
			"offset[%d] begin[%d] end[%d]", off, m.begin, m.end)
	}
	if off == m.end {
		return 0, io.EOF
	}
	offset := off - m.begin
	index := offset / int64(m.blockSize)
	offset = offset % int64(m.blockSize)

	var ret int
	for len(p) > 0 {
		block := &m.blocks[index]
		n := copy(p, block.buf[offset:block.limit])
		offset = 0
		ret += n
		p = p[n:]
		index++
		if index >= int64(len(m.blocks)) {
			break
		}
	}
	if ret == 0 {
		return 0, io.EOF
	}
	return ret, nil
}

func (m *mStream) write(p []byte) int64 {
	m.locker.Lock()
	defer m.locker.Unlock()
	for len(p) > 0 {
		if m.blocks[len(m.blocks)-1].limit == m.blockSize {
			m.blocks = append(m.blocks, makeBlock(m.end, m.blockSize))
		}
		block := &m.blocks[len(m.blocks)-1]
		n := copy(block.buf[block.limit:], p)
		block.limit += n
		m.end += int64(n)
		p = p[n:]
	}
	return m.end
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
	return n, nil
}

type block struct {
	limit int
	begin int64
	buf   []byte
}

func makeBlock(begin int64, blockSize int) block {
	return block{
		limit: 0,
		begin: begin,
		buf:   make([]byte, blockSize),
	}
}

func (block *block) writeTo(writer io.Writer) (int, error) {
	return writer.Write(block.buf[:block.limit])
}
