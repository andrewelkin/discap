package DataNode

import (
	"container/list"
	"sync"
)

type dataEntry struct {
	key   string // element's key
	value any    // the data
}

type SingleDataNode struct {
	sync.RWMutex
	data    *list.List // we will keep fresh data in the front, we will kill old data from the back
	maxSize int
}

func (n *SingleDataNode) New(maxSize int) *SingleDataNode {
	n.data = list.New()
	n.maxSize = maxSize
	return n
}

// private
func (n *SingleDataNode) findLinear(key string) (*dataEntry, bool) {
	// Simply iterate through the list, O(1)
	for e := n.data.Front(); e != nil; e = e.Next() {
		if e.Value.(*dataEntry).key == key {
			return e.Value.(*dataEntry), true
		}
	}
	return nil, false
}

// public
func (n *SingleDataNode) FindLinear(key string) (any, bool) {

	de, ok := n.findLinear(key)
	if ok {
		return de.value, true
	}
	return nil, false
}

// todo: map search methods

// true: success story
func (n *SingleDataNode) MaybePushNew(key string, value any) (*dataEntry, bool) {

	de, ok := n.findLinear(key)
	if ok {
		return de, false // element exists already
	}
	pair := &dataEntry{ // make a new pair
		key:   key,
		value: value,
	}

	// check the size
	if n.data.Len() >= n.maxSize {
		// the list is full, we got to kill the back element. // maybe: once this limit is reached we always kill, without calling Len()
		n.data.Remove(n.data.Back())
	}

	n.data.PushFront(pair) // push it to the list
	return pair, true
}
