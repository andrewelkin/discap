package DataNode

import (
	"container/list"
	"context"
	"fmt"

	"sync"
)

type dataEntry struct {
	key         string // element's key
	value       any    // the data
	useCounterR int64  // number of reads
	useCounterW int64  // number of writes

}

type DNRequest struct {
	Command string
	Keys    []string
	Values  []any
	BackCh  chan DNResponse
}

type DNResponse struct {
	Status  string
	Message string
	Count   int
	Keys    []string
	Values  []any
}

const queueSize = 100

type SingleDataNode struct {
	sync.Mutex
	ctx     context.Context
	dataCh  chan DNRequest
	data    *list.List // we will keep fresh data in the front, we will kill old data from the back
	maxSize int
}

func (n *SingleDataNode) New(ctx context.Context, maxSize int) *SingleDataNode {
	n.data = list.New()
	n.ctx = ctx
	n.maxSize = maxSize
	n.dataCh = make(chan DNRequest, queueSize)
	go n.mainLoop()
	return n
}

func (n *SingleDataNode) GetChannel() chan DNRequest {
	return n.dataCh
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
func (n *SingleDataNode) findSingleKey(key string) (any, int64, int64, bool) {

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

func (n *SingleDataNode) findMultipleKeys(keys []string) (resKeys []string, resValues []any) {

	n.Lock()
	defer n.Unlock()
	if n.data.Len() > 0 {
		mkeys := make(map[string]int)
		for _, key := range keys {
			mkeys[key] = 0
		}

		var needTouch []*list.Element
		for e := n.data.Front(); e != nil; e = e.Next() {
			if _, ok := mkeys[e.Value.(*dataEntry).key]; ok {
				e.Value.(*dataEntry).useCounterR += 1
				needTouch = append(needTouch, e)
				resKeys = append(resKeys, e.Value.(*dataEntry).key)
				resValues = append(resValues, e.Value.(*dataEntry).value)
			}
		}

		for _, e := range needTouch {
			n.data.MoveToFront(e)
		}

	}
	return resKeys, resValues
}

// true: new element
func (n *SingleDataNode) putSingle(key string, value any) bool {

	n.Lock()
	defer n.Unlock()
	e, ok := n.findLinear(key)
	if ok {
		de := e.Value.(*dataEntry)
		de.useCounterW++
		de.value = value
		n.data.MoveToFront(e)
		return false // element exists already, update and make most recent
	}
	// check if there is space
	if n.data.Len() >= n.maxSize {
		// the list is full, we got to kill the back element.
		// note: once this limit is reached we always kill, without calling Len(), do we save anything?
		n.data.Remove(n.data.Back())
	}
	n.data.PushFront(&dataEntry{ // make a new pair and push it as the most recent
		key:         key,
		value:       value,
		useCounterW: 1,
	})
	return true
}

func (n *SingleDataNode) maybePushMultiple(keys []string, values []any) error {

	if len(values) != len(keys) {
		return fmt.Errorf("bad keys/values/ndx array dimensions %d/%d", len(keys), len(values))
	}
	for i, k := range keys {
		go n.putSingle(k, values[i])
	}
	return nil
}

func (n *SingleDataNode) invalid(keys ...string) {

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

func (n *SingleDataNode) invalidAll() (count int) {
	n.Lock()
	defer n.Unlock()
	count = n.data.Len()
	n.data = list.New()
	return
}

func (n *SingleDataNode) Len() int {
	n.Lock()
	defer n.Unlock()
	return n.data.Len()
}

func (n *SingleDataNode) mainLoop() {

	for {
		select {
		case <-n.ctx.Done(): // user cancellation
			return
		case rq := <-n.dataCh:
			if rq.Command == "del" {
				count := n.invalidAll()
				rq.BackCh <- DNResponse{
					Status:  "OK",
					Count:   count,
					Message: fmt.Sprintf("deleted %d records", count),
				}

			} else if rq.Command == "put" {
				//fmt.Printf("putting %d records\n", len(rq.Keys))
				err := n.maybePushMultiple(rq.Keys, rq.Values)
				if err != nil {
					//fmt.Printf("error: %s\n", err.Error())
					rq.BackCh <- DNResponse{
						Status:  "Error",
						Message: err.Error(),
					}
				} else {
					//fmt.Printf("stored %d records\n", len(rq.Keys))
					rq.BackCh <- DNResponse{
						Status:  "OK",
						Message: fmt.Sprintf("stored %d records", len(rq.Keys)),
						Count:   len(rq.Keys),
					}
				}
			} else if rq.Command == "get" {
				//fmt.Printf("getting %d records\n", len(rq.Keys))

				if len(rq.Keys) == 0 {
					rq.BackCh <- DNResponse{
						Status: "OK",
						Count:  n.Len(),
					}

				} else {
					resKeys, resValues := n.findMultipleKeys(rq.Keys)
					rq.BackCh <- DNResponse{
						Status: "OK",
						Keys:   resKeys,
						Values: resValues,
					}
				}
			}
		}
	}
}
