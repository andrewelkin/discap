package DataNode

import (
	"container/list"
	"sync"
)

type dataEntry struct {
	key         string // element's key
	value       any    // the data
	useCounterR int64  // number of reads
	useCounterW int64  // number of writes
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

func (n *SingleDataNode) findLinear(key string) (*list.Element, bool) {
	// Simply iterate through the list, O(n)
	for e := n.data.Front(); e != nil; e = e.Next() {
		if e.Value.(*dataEntry).key == key {
			return e, true
		}
	}
	return nil, false
}

// public, value + counters
func (n *SingleDataNode) FindSingleKey(key string) (any, int64, int64, bool) {

	e, ok := n.findLinear(key)
	if ok {
		de := e.Value.(*dataEntry)
		de.useCounterR += 1
		n.data.MoveToFront(e)
		return de.value, de.useCounterR, de.useCounterW, true
	}
	return nil, 0, 0, false
}

func (n *SingleDataNode) FindMultipleKeys(keys ...string) []*dataEntry {

	var res []*dataEntry
	if n.data.Len() > 0 {
		mkeys := make(map[string]any)
		for _, key := range keys {
			mkeys[key] = nil
		}
		for e := n.data.Front(); e != nil; e = e.Next() {
			if _, ok := mkeys[e.Value.(*dataEntry).key]; ok {
				e.Value.(*dataEntry).useCounterR += 1
				n.data.MoveToFront(e)
				res = append(res, e.Value.(*dataEntry))
			}
		}
	}
	return res
}

// todo: map search methods

// true: new element
func (n *SingleDataNode) PutSingle(key string, value any) bool {

	e, ok := n.findLinear(key)
	if ok {
		de := e.Value.(*dataEntry)
		de.useCounterW++
		de.value = value
		n.data.MoveToFront(e)
		return false // element exists already
	}
	pair := &dataEntry{ // make a new pair
		key:         key,
		value:       value,
		useCounterW: 1,
	}

	// check the size
	if n.data.Len() >= n.maxSize {
		// the list is full, we got to kill the back element.
		//maybe: once this limit is reached we always kill, without calling Len()
		n.data.Remove(n.data.Back())
	}

	n.data.PushFront(pair) // push it to the list
	return true
}

// ret how new many added
func (n *SingleDataNode) MaybePushMultiple(newPairs map[string]any) int {

	var count int
	for k, v := range newPairs {
		if n.PutSingle(k, v) {
			count++
		}
	}
	return count
}

func (n *SingleDataNode) Invalid(keys ...string) {

	mkeys := make(map[string]any)
	for _, key := range keys {
		mkeys[key] = nil
	}
	for e := n.data.Front(); e != nil; {
		current := e
		e = e.Next()
		if _, ok := mkeys[current.Value.(*dataEntry).key]; ok {
			n.data.Remove(current)
		}
	}
}

func (n *SingleDataNode) InvalidAll() {
	n.data = list.New()
}
