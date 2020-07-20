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

import "log"

type wWriter struct {
	wal    *wal
	queue  *entryQueue
	commit *entryQueue
}

//append the entry to the queue of writer
func (worker *wWriter) append(e *entry) {
	worker.queue.put(e)
}

func (worker *wWriter) start() {
	go func() {
		for {
			var commit = entriesPool.Get().([]*entry)[0:]
			entries := worker.queue.take()
			for i := range entries {
				e := entries[i]
				if err := worker.wal.write(e); err != nil {
					e.cb(err)
				} else {
					commit = append(commit, e)
				}
			}
			if len(commit) > 0 {
				if err := worker.wal.flush(); err != nil {
					log.Fatal(err.Error())
				}
				worker.commit.putEntries(commit)
			}
		}
	}()
}