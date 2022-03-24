package concurrency

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

type ChildSet struct {
	accessLock sync.Mutex

	children map[ResourceNamePart]*MultiLevelLock
	// map[ResourceNamePart]struct{} used as a set
	txChildren map[TransactionId]map[ResourceNamePart]struct{}
}

func NewTransactionRefCounter() *ChildSet {
	return &ChildSet{
		children:   make(map[ResourceNamePart]*MultiLevelLock),
		txChildren: make(map[TransactionId]map[ResourceNamePart]struct{}),
	}
}

func (t *ChildSet) AddChild(ptr *MultiLevelLock) {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	childKey := ptr.lastNamePart
	if v, ok := t.children[childKey]; ok && v != ptr {
		panic(errors.Errorf("cannot add child '%s' twice", childKey))
	}
	t.children[childKey] = ptr
}

func (t *ChildSet) RemoveChild(childKey ResourceNamePart) {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	if _, ok := t.children[childKey]; ok {
		panic(errors.Errorf("child '%s' does not exist", childKey))
	}
	delete(t.children, childKey)
}

func (t *ChildSet) AddReference(txId TransactionId, childKey ResourceNamePart) {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	if _, ok := t.children[childKey]; !ok {
		panic(errors.Errorf("child '%s' does not exist", childKey))
	}
	if _, ok := t.txChildren[txId]; !ok {
		t.txChildren[txId] = make(map[ResourceNamePart]struct{})
	}
	t.txChildren[txId][childKey] = struct{}{}
}

func (t *ChildSet) RemoveReference(txId TransactionId, childKey ResourceNamePart) {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	if _, ok := t.children[childKey]; !ok {
		panic(errors.Errorf("child '%s' does not exist", childKey))
	}
	if _, ok := t.txChildren[txId]; !ok {
		t.txChildren[txId] = make(map[ResourceNamePart]struct{})
	}
	delete(t.txChildren[txId], childKey)
}

func (t *ChildSet) ClearReferences(txId TransactionId) {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()
	delete(t.txChildren, txId)
}

func (t *ChildSet) TransactionCount(txId TransactionId) int {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	if _, ok := t.txChildren[txId]; ok {
		return len(t.txChildren[txId])
	}
	return 0
}

func (t *ChildSet) TransactionChildren(txId TransactionId) []*MultiLevelLock {
	t.accessLock.Lock()
	defer t.accessLock.Unlock()

	if children, ok := t.txChildren[txId]; ok {
		ret := make([]*MultiLevelLock, 0, len(children))
		for child := range children {
			if _, childOk := t.children[child]; !childOk {
				panic(errors.Errorf("child '%s' does not exist", child))
			}
			ret = append(ret, t.children[child])
		}
		return ret
	}
	return []*MultiLevelLock{}
}

func (t *ChildSet) String() string {
	return fmt.Sprintf("TransactionRefCounter(%d children, %d tx)", len(t.children), len(t.txChildren))
}
