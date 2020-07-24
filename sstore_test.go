package sstore

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
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
	if _, err := sstore.Append("hello", []byte("hello world")); err != nil {
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
			sstore.AsyncAppend(name, []byte(data), func(offset int64, err error) {
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
		sstore.AsyncAppend(name, d, func(pos int64, err error) {
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
	reader, err := sstore.Reader(name)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	n, err := reader.Read(buffer)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if n != len(buffer) {
		t.Fatal(n)
	}
	crc32W := crc32.NewIEEE()
	crc32W.Write(buffer)
	if crc32W.Sum32() != sum32 {
		t.Fatal(sum32)
	}

	reader, _ = sstore.Reader(name)
	readAllData, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	crc32W = crc32.NewIEEE()
	crc32W.Write(readAllData)
	if crc32W.Sum32() != sum32 {
		t.Fatal(sum32)
	}

	sstore.Close()
}

func TestSStore_Watcher(t *testing.T) {
	os.RemoveAll("data")
	sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(MB))
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var name = "stream1"
	var data = "hello world"
	if _, err := sstore.Append(name, []byte(data)); err != nil {
		t.Fatalf("%+v", err)
	}

	go func() {
		reader, err := sstore.Reader(name)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		readAll, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if string(readAll) != data {
			t.Fatalf("%s %s", string(readAll), data)
		}

		readAll, err = ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if len(readAll) > 0 {
			t.Fatalf("reader no data remain")
		}

		watcher := sstore.Watcher(name)
		defer watcher.Close()

		select {
		case pos := <-watcher.Watch():
			fmt.Println("pos", pos)
		}

		readAll, err = ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if string(readAll) != "hello world2" {
			t.Fatalf("%s ", string(readAll))
		}
	}()

	time.Sleep(time.Second)
	if _, err := sstore.Append(name, []byte("hello world2")); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := sstore.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}
