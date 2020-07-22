package sstore

import (
	"sync"
	"sync/atomic"
)

type endWatcher struct {
	index  int64
	c      func()
	int64s chan int64
}

type notify struct {
	name string
	end  int64
}

type endWatchers struct {
	watchIndex     int64
	endWatcherMap  map[string][]endWatcher
	endWatcherLock *sync.RWMutex

	l           *sync.Mutex
	cond        *sync.Cond
	notifyItems []*notify
	isClose     int32
}

var notifyItemPool = sync.Pool{New: func() interface{} {
	return new(notify)
}}

func newEndWatchers() *endWatchers {
	l := new(sync.Mutex)
	return &endWatchers{
		watchIndex:     0,
		l:              l,
		cond:           sync.NewCond(l),
		endWatcherLock: new(sync.RWMutex),
		notifyItems:    make([]*notify, 1024),
		endWatcherMap:  make(map[string][]endWatcher),
	}
}
func (endWatchers *endWatchers) removeEndWatcher(index int64, name string) {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatcherS, ok := endWatchers.endWatcherMap[name]
	if ok == false {
		return
	}
	for i, watcher := range endWatcherS {
		if watcher.index == index {
			copy(endWatcherS[i:], endWatcherS[i+1:])
			endWatcherS[len(endWatcherS)-1] = endWatcher{}
			endWatcherS = endWatcherS[:len(endWatcherS)-1]
			endWatchers.endWatcherMap[name] = endWatcherS
			break
		}
	}
}

func (endWatchers *endWatchers) newEndWatcher(name string) *endWatcher {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatchers.watchIndex++
	index := endWatchers.watchIndex
	watcher := endWatcher{
		index:  index,
		int64s: make(chan int64, 1),
		c: func() {
			endWatchers.removeEndWatcher(index, name)
		},
	}
	endWatchers.endWatcherMap[name] = append(endWatchers.endWatcherMap[name], watcher)
	return &watcher
}

func (endWatchers *endWatchers) getEndWatcher(name string) []endWatcher {
	endWatchers.endWatcherLock.RLock()
	watcher, _ := endWatchers.endWatcherMap[name]
	endWatchers.endWatcherLock.RUnlock()
	return watcher
}

func (endWatchers *endWatchers) take(buf []*notify) []*notify {
	endWatchers.l.Lock()
	defer endWatchers.l.Unlock()
	for len(endWatchers.notifyItems) == 0 {
		endWatchers.cond.Wait()
	}
	notifyItems := endWatchers.notifyItems
	endWatchers.notifyItems = buf[:0]
	return notifyItems
}

func (endWatchers *endWatchers) start() {
	go func() {
		var buf = make([]*notify, 128)
		for {
			items := endWatchers.take(buf)
			if atomic.LoadInt32(&endWatchers.isClose) == 1 {
				return
			}
			for _, item := range items {
				for _, watcher := range endWatchers.getEndWatcher(item.name) {
					watcher.notify(item.end)
				}
				notifyItemPool.Put(item)
			}
			buf = items
		}
	}()
}

func (endWatchers *endWatchers) close() {
	atomic.StoreInt32(&endWatchers.isClose, 1)
	endWatchers.notify(&notify{
		name: "",
		end:  0,
	})
}

func (endWatchers *endWatchers) notify(item *notify) {
	endWatchers.l.Lock()
	endWatchers.notifyItems = append(endWatchers.notifyItems, item)
	endWatchers.l.Unlock()
	endWatchers.cond.Signal()
}

func (watcher *endWatcher) notify(pos int64) {
	select {
	case watcher.int64s <- pos:
	default:
	}
}

func (watcher *endWatcher) Watch() chan int64 {
	return watcher.int64s
}

func (watcher *endWatcher) Close() {
	if watcher.c != nil {
		watcher.c()
		watcher.c = nil
	}
}
