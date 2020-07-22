package sstore

import (
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	os.RemoveAll("data")
	sstore, err := Open(DefaultOptions("data"))
	if err != nil {
		t.Fatalf("%+v",err)
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
}

func TestRecover(t *testing.T) {
	sstore, err := Open(DefaultOptions("data"))
	if err != nil {
		t.Fatal(err.Error())
	}
	pos, ok := sstore.End("hello")
	if ok == false {
		t.Fatal(ok)
	}
	if pos != int64(len("hello world")) {
		t.Fatal(pos)
	}
}
