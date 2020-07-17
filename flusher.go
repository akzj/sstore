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

type flusher struct {
	items chan func()
}

func newFlusher() {

}

func (flusher *flusher) append(table *mStreamTable, cb func(err error)) {
	flusher.items <- func() {
		go cb(flusher.flushMStreamTable(table))
	}
}

func (flusher *flusher) flushMStreamTable(table *mStreamTable) error {
	return nil
}

func (flusher *flusher) start() {
	go func() {
		for {
			select {
			case f := <-flusher.items:
				f()
			}
		}
	}()
}
