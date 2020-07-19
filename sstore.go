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
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
)

type SStore struct {
	entryQueueSize uint32
	entryQueues    []entryQueue

	segments map[string]*segment

	entryID    int64
	notifyPool sync.Pool

	committer *committer
}

func (sstore *SStore) nextEntryID() int64 {
	return atomic.AddInt64(&sstore.entryID, 1)
}

func (sstore *SStore) getEntryQueue(name string) *entryQueue {
	return &sstore.entryQueues[crc32.ChecksumIEEE([]byte(name))/sstore.entryQueueSize]
}

//Append append the data to end of the stream
func (sstore *SStore) Append(name string, data []byte) error {
	notify := sstore.notifyPool.Get().(chan interface{})
	var err error
	sstore.AsyncAppend(name, data, func(e error) {
		err = e
		notify <- struct{}{}
	})
	<-notify
	sstore.notifyPool.Put(notify)
	return err
}

//AsyncAppend async append the data to end of the stream
func (sstore *SStore) AsyncAppend(name string, data []byte, cb func(err error)) {
	sstore.getEntryQueue(name).put(&entry{
		ID:   sstore.nextEntryID(),
		name: name,
		data: data,
		cb:   cb,
	})
}

//ReadSeeker create ReadSeeker of the stream
func (sstore *SStore) ReadSeeker(name string, pos int) io.ReadSeeker {
	return nil
}

//Watcher create watcher of the stream
func (sstore *SStore) Watcher(name string) Watcher {
	return nil
}

//size return the end of stream.
//return -1 when the stream no exist
func (sstore *SStore) End(name string) int64 {
	return -1
}

//base return the begin of stream.
//return -1 when the stream no exist
func (sstore *SStore) Begin(name string) int64 {
	return -1
}

//Exist
//return true if the stream exist otherwise return false
func (sstore *SStore) Exist() bool {
	return false
}
