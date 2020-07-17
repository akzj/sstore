package sstore

import "sync"

type committer struct {
	queue *entryQueue

	mSize  int64
	locker sync.Mutex
}

func (c *committer) getMstream(name string) *mstream {
	return nil
}

func (c *committer) tryFlush() {

}

func (c *committer) start() {
	go func() {
		for {
			var entryBuffer = entriesPool.Get().([]entry)
			entries := c.queue.take(entryBuffer)
			for _, e := range entries {
				n, _ := c.getMstream(e.name).Write(e.data)
				c.mSize += int64(n)
				c.tryFlush()
			}
		}
	}()
}
