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

import "fmt"

type cbWorker struct {
	queue *entryQueue
}

func newCbWorker(queue *entryQueue) *cbWorker {
	return &cbWorker{queue: queue}
}

func (worker *cbWorker) start() {
	go func() {
		for {
			entries := worker.queue.take()
			for index := range entries {
				e := entries[index]
				if e.ID == closeSignal {
					e.cb(0, nil)
					return
				}
				if e == nil {
					panic(e)
				}
				if e.cb == nil {
					panic(fmt.Sprintf("entry.ID %d entry.streamID%d", e.ID, e.StreamID))
				}
				e.cb(e.end, e.err)
			}
			entriesPool.Put(entries[:0])
		}
	}()
}
