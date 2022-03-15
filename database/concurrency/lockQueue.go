package concurrency

import (
	"sync"

	"github.com/gammazero/deque"
)

type lockRequest struct {
	Ack  chan<- bool
	Lock Lock
}

type lockQueue struct {
	queue *deque.Deque
	lock  sync.Mutex
}

func newLockQueue() lockQueue {
	return lockQueue{
		queue: deque.New(),
	}
}

func (l *lockQueue) PushFront(entry lockRequest) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.queue.PushFront(entry)
}

func (l *lockQueue) PushEnd(entry lockRequest) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.queue.PushBack(entry)
}

func (l *lockQueue) Pop() *lockRequest {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.queue.Len() == 0 {
		return nil
	}
	popped := l.queue.PopFront().(lockRequest)
	return &popped
}

func (l *lockQueue) PopMatching(matcher func(item *lockRequest) bool) *lockRequest {
	l.lock.Lock()
	defer l.lock.Unlock()

	index := l.queue.Index(func(i interface{}) bool {
		entry := i.(lockRequest)
		return matcher(&entry)
	})
	if index == -1 {
		return nil
	}
	popped := l.queue.Remove(index).(lockRequest)
	return &popped
}

func (l *lockQueue) HasNext() bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.queue.Len() != 0
}
