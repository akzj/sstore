package sstore

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestOpen(t *testing.T) {
	_ = os.RemoveAll("data")
	defer func() {
		_ = os.RemoveAll("data")
	}()
	sstore, err := Open(DefaultOptions("data"))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if sstore.committer.mutableMStreamMap == nil {
		t.Fatal(sstore.committer.mutableMStreamMap)
	}
	if err := sstore.Append("hello", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	pos, ok := sstore.End("hello")
	if !ok {
		t.Fatal(ok)
	}
	if len("hello world") != int(pos) {
		t.Fatal(pos)
	}
	_ = sstore.Close()
}

func TestRecover(t *testing.T) {
	_ = os.RemoveAll("data")
	defer func() {
		_ = os.RemoveAll("data")
	}()
	for i := 0; i < 10; i++ {
		sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(10 * MB))
		if err != nil {
			t.Fatal(err.Error())
		}
		var name = "stream1"
		var data = strings.Repeat("hello world,", 10)
		var wg sync.WaitGroup
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			sstore.AsyncAppend(name, []byte(data), func(err error) {
				if err != nil {
					t.Fatalf("%+v", err)
				}
				wg.Done()
			})
		}
		wg.Wait()
		if err := sstore.Close(); err != nil {
			t.Errorf("%+v", err)
		}

		sstore, err = Open(DefaultOptions("data").WithMaxMStreamTableSize(10 * MB))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := sstore.Close(); err != nil {
			t.Errorf("%+v", err)
		}
	}
}

func TestWalHeader(t *testing.T) {
	os.RemoveAll("data")
	defer func() {
		os.RemoveAll("data")
	}()
	os.MkdirAll("data", 0777)
	wal, err := openWal("data/1.log")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := wal.write(&entry{ID: 1000}); err != nil {
		t.Fatalf("%+v", err)
	}
	header := wal.getHeader()
	if header.LastEntryID != 1000 {
		t.Fatalf("%d %d", wal.getHeader().LastEntryID, 1000)
	}
	if header.Filename != "1.log" {
		t.Fatalf(header.Filename)
	}
}

func TestReader(t *testing.T) {
	os.RemoveAll("data")
	sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(MB))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var name = "stream1"
	var data = strings.Repeat("hello world,", 10)
	var wg sync.WaitGroup

	writer := crc32.NewIEEE()

	for i := 0; i < 100000; i++ {
		wg.Add(1)
		d := []byte(data)
		_, _ = writer.Write(d)
		sstore.AsyncAppend(name, d, func(err error) {
			if err != nil {
				t.Fatalf("%+v", err)
			}
			wg.Done()
		})
	}
	wg.Wait()

	sum32 := writer.Sum32()
	size, ok := sstore.End(name)
	if ok == false {
		t.Fatal(ok)
	}

	for _, it := range sstore.indexTable.get(name).items {
		if it.mStream != nil {
			fmt.Printf("mStream [%d-%d) \n", it.mStream.begin, it.mStream.end)
		} else
		if it.segment != nil {
			info := it.segment.meta.OffSetInfos[name]
			fmt.Printf("segment begin [%d-%d) \n", info.Begin, info.End)
		}
	}

	info, err := sstore.indexTable.get(name).find(521)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if info.begin > 521 || info.end < 512 {
		t.Fatalf("%+v", info)
	}

	var buffer = make([]byte, size)
	n, err := sstore.ReadSeeker(name).Read(buffer)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if n != len(buffer) {
		t.Fatal(n)
	}
	reader := crc32.NewIEEE()
	reader.Write(buffer)
	if reader.Sum32() != sum32 {
		t.Fatal(sum32)
	}

	readAllData, err := ioutil.ReadAll(sstore.ReadSeeker(name))
	if err != nil {
		t.Fatalf("%+v", err)
	}

	reader = crc32.NewIEEE()
	reader.Write(readAllData)
	if reader.Sum32() != sum32 {
		t.Fatal(sum32)
	}

	sstore.Close()
}
