package DataNode

import (
	"container/list"
	"fmt"
	"sync"
)

type dataEntry struct {
	key         string // element's key
	value       any    // the data
	useCounterR int64  // number of reads
	useCounterW int64  // number of writes
}

type SingleDataNode struct {
	sync.Mutex
	data    *list.List // we will keep fresh data in the front, we will kill old data from the back
	maxSize int
}

func (n *SingleDataNode) New(maxSize int) *SingleDataNode {
	n.data = list.New()
	n.maxSize = maxSize
	return n
}

// private
// todo: map search methods

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
	n.Lock()
	defer n.Unlock()

	e, ok := n.findLinear(key)
	if ok {
		de := e.Value.(*dataEntry)
		de.useCounterR += 1
		n.data.MoveToFront(e)
		return de.value, de.useCounterR, de.useCounterW, true
	}
	return nil, 0, 0, false
}

func (n *SingleDataNode) FindMultipleKeysAr(keys []string) map[string]any {

	n.Lock()
	defer n.Unlock()
	var res map[string]any
	if n.data.Len() > 0 {
		res = make(map[string]any)
		mkeys := make(map[string]int)
		for _, key := range keys {
			mkeys[key] = 0
		}
		for e := n.data.Front(); e != nil; e = e.Next() {
			if _, ok := mkeys[e.Value.(*dataEntry).key]; ok {
				e.Value.(*dataEntry).useCounterR += 1
				n.data.MoveToFront(e)
				res[e.Value.(*dataEntry).key] = e.Value.(*dataEntry).value
			}
		}
	}
	return res
}

func (n *SingleDataNode) FindMultipleKeys(keys ...string) map[string]any {

	n.Lock()
	defer n.Unlock()
	var res map[string]any
	if n.data.Len() > 0 {
		res = make(map[string]any)
		mkeys := make(map[string]int)
		for _, key := range keys {
			mkeys[key] = 0
		}
		for e := n.data.Front(); e != nil; e = e.Next() {
			if _, ok := mkeys[e.Value.(*dataEntry).key]; ok {
				e.Value.(*dataEntry).useCounterR += 1
				n.data.MoveToFront(e)
				res[e.Value.(*dataEntry).key] = e.Value.(*dataEntry).value
			}
		}
	}
	return res
}

// true: new element
func (n *SingleDataNode) PutSingle(key string, value any) bool {

	n.Lock()
	defer n.Unlock()
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

func (n *SingleDataNode) MaybePushMultipleAr(keys []string, values []any) error {

	if len(values) != len(keys) {
		return fmt.Errorf("bad keys/values array dimensions %d/%d", len(keys), len(values))
	}
	for i, k := range keys {
		go n.PutSingle(k, values[i])
	}

	return nil
}

func (n *SingleDataNode) MaybePushMultiple(newPairs map[string]any) {
	for k, v := range newPairs {
		go n.PutSingle(k, v)
	}
}

func (n *SingleDataNode) Invalid(keys ...string) {

	n.Lock()
	defer n.Unlock()
	mkeys := make(map[string]int)
	for _, key := range keys {
		mkeys[key] = 0
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
	n.Lock()
	defer n.Unlock()
	n.data = list.New()
}

func (n *SingleDataNode) Len() int {
	n.Lock()
	defer n.Unlock()
	return n.data.Len()
}
