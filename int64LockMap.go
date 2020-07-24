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

import "sync"

type int64LockMap struct {
	locker *sync.RWMutex
	sizes  map[string]int64
}

func newInt64LockMap() *int64LockMap {
	return &int64LockMap{
		locker: new(sync.RWMutex),
		sizes:  make(map[string]int64, 1024),
	}
}

func (sizeMap *int64LockMap) set(name string, pos int64) {
	sizeMap.locker.Lock()
	sizeMap.sizes[name] = pos
	sizeMap.locker.Unlock()
}

func (sizeMap *int64LockMap) get(name string) (int64, bool) {
	sizeMap.locker.RLock()
	size, ok := sizeMap.sizes[name]
	sizeMap.locker.RUnlock()
	return size, ok
}
