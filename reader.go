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

import "io"

type reader struct {
	offset int64
	name   string
	index  *offsetIndex
}

func newReader(name string, offset int64, index *offsetIndex) *reader {
	return &reader{
		name:   name,
		index:  index,
		offset: offset,
	}
}

func (r *reader) Read(p []byte) (n int, err error) {
	buf := p
	var ret int
	for len(buf) > 0 {
		item, err := r.index.find(r.offset)
		if err != nil {
			return 0, err
		}
		if item.mStream != nil {
			n, err := item.mStream.ReadAt(buf, r.offset)
			if err != nil && err != io.EOF {
				return ret, err
			}
			ret += n
			buf = p[:ret]
			if item.mStream.end == mStreamEnd {
				return ret, nil
			}
		} else if item.segment != nil {
			if item.segment.refInc() < 0 {
				return ret, errOffSet
			}
			n, err := item.segment.Reader(r.name).ReadAt(buf, r.offset)
			item.segment.refDec()
			if err != nil {
				return ret, err
			}
			ret += n
			buf = p[:ret]
		}
	}
	return ret, nil
}