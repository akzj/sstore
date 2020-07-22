package sstore

import (
	"os"
	"strings"
	"sync"
	"testing"
)

func TestOpen(t *testing.T) {
	_ = os.RemoveAll("data")
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
	sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(10 * MB))
	if err != nil {
		t.Fatal(err.Error())
	}
	var name = "stream1"
	var data = strings.Repeat("hello world,", 10)
	var wg sync.WaitGroup
	for i := 0; i < 1000000; i++ {
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
}

func TestRecover2(t *testing.T) {
	sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(MB))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	//fmt.Println(sstore.End("stream1"))
	if err := sstore.Close(); err != nil {
		t.Errorf("%+v", err)
	}
}

func TestWalHeader(t *testing.T) {
	os.RemoveAll("data")
	os.MkdirAll("data",0777)
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
