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
	"math"
	"sync"
)

//memory stream
type mStream struct {
	locker sync.RWMutex
	name   string
	begin  int64
	end    int64
	size   int64
	blocks []block
}

const mStreamEnd = math.MaxInt64

func newMStream(begin int64, name string) *mStream {
	blocks := make([]block, 0, 128)
	blocks = append(blocks, makeBlock(begin))
	return &mStream{
		name:   name,
		begin:  begin,
		size:   0,
		blocks: blocks,
		end:    mStreamEnd,
	}
}

func (m *mStream) ReadAt(p []byte, off int64) (n int, err error) {
	m.locker.RLock()
	defer m.locker.RUnlock()
	if off < m.begin || off >= m.end {
		return 0, errOffSet
	}
	if len(m.blocks) == 0 {
		return 0, io.EOF
	}
	offset := off - m.begin
	index := offset / blockSize
	offset = offset % blockSize
	if index >= int64(len(m.blocks)) {
		return 0, errOffSet
	}

	buf := p
	var ret int
	for len(buf) > 0 {
		block := &m.blocks[index]
		n := copy(buf, block.buf[:block.limit])
		ret += n
		buf = buf[n:]
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
		if m.blocks[len(m.blocks)-1].limit == blockSize {
			m.blocks = append(m.blocks, makeBlock(m.begin+m.size))
		}
		block := &m.blocks[len(m.blocks)-1]
		n := copy(block.buf[block.limit:], p)
		block.limit += n
		m.size += int64(n)
		p = p[n:]
	}
	return m.begin + m.size
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

func (m *mStream) close() {
	m.locker.Lock()
	defer m.locker.Unlock()
	m.end = m.begin + m.size
}

const blockSize = 4 * 1024

type block struct {
	limit int
	begin int64
	buf   []byte
}

func makeBlock(begin int64) block {
	return block{
		limit: 0,
		begin: begin,
		buf:   make([]byte, blockSize),
	}
}

func (block *block) writeTo(writer io.Writer) (int, error) {
	return writer.Write(block.buf[:block.limit])
}
