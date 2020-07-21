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
	"math"
	"path/filepath"
)

type Options struct {
	Path                          string `json:"path"`
	FilesDir                      string `json:"files_dir"`
	WalDir                        string `json:"wal_dir"`
	SegmentDir                    string `json:"segment_dir"`
	MaxSegmentCount               int64  `json:"max_segment_count"`
	BlockSize                     int64  `json:"block_size"`
	MaxMStreamTableSize           int64  `json:"max_mStream_table_size"`
	MaxImmutableMStreamTableCount int    `json:"max_immutable_mStream_table_count"`
	EntryQueueCap                 int    `json:"entry_queue_cap"`
}

func DefaultOptions(Path string) Options {
	return Options{
		Path:                          Path,
		FilesDir:                      filepath.Join(Path, "files"),
		WalDir:                        filepath.Join(Path, "wals"),
		SegmentDir:                    filepath.Join(Path, "segments"),
		MaxSegmentCount:               math.MaxInt64,
		BlockSize:                     4 * 1024,
		MaxMStreamTableSize:           256 * 1024 * 1024,
		MaxImmutableMStreamTableCount: 4,
		EntryQueueCap:                 128,
	}
}

func (opt Options) WithFilesDir(path string) Options {
	opt.FilesDir = path
	return opt
}

func (opt Options) WithSegmentDir(path string) Options {
	opt.SegmentDir = path
	return opt
}

func (opt Options) WithWalPath(path string) Options {
	opt.WalDir = path
	return opt
}

func (opt Options) WithMaxSegmentCount(MaxSegmentCount int64) Options {
	opt.MaxSegmentCount = MaxSegmentCount
	return opt
}
