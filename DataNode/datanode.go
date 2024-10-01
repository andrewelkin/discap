package DataNode

import (
	"container/list"
	"context"
	"fmt"

	"sync"
)

// internal store element
type dataEntry struct {
	key         string // element's key
	value       any    // the data
	useCounterR int64  // number of reads
	useCounterW int64  // number of writes

}

// DNRequest is a request struct sent from manager to the node
type DNRequest struct {
	Command string          // one of the "get" "put" "del"
	Keys    []string        // array of keys
	Values  []any           // array of values
	BackCh  chan DNResponse // channel to reply
}

// DNResponse response struct from a node to the cache manager
type DNResponse struct {
	Status  string   // "OK" or "Error" for good or bad cases
	Message string   // text to read
	Count   int      // generally a number of single ops (i.e. records saved or deleted)
	Keys    []string // found records keys
	Values  []any    // their values
}

const queueSize = 100

// SingleDataNode data node class
type SingleDataNode struct {
	sync.Mutex                          // lock for concurrent ops
	ctx        context.Context          // exec context with cancel
	dataCh     chan DNRequest           // channel to receive requests
	data       *list.List               // data storage. Note: we will keep fresh data in the front, we will kill old data from the back
	dataMap    map[string]*list.Element // map of the elements
	maxSize    int                      // node capacity
}

// New  constructs a node
// --> Input:
// ctx         context.Context     execution context
// maxSize     int                 node max size
// <-- Output:
// 1) *SingleDataNode     initialized node
func (n *SingleDataNode) New(ctx context.Context, maxSize int) *SingleDataNode {
	n.data = list.New()
	n.dataMap = make(map[string]*list.Element)
	n.ctx = ctx
	n.maxSize = maxSize
	n.dataCh = make(chan DNRequest, queueSize)
	go n.mainLoop()
	return n
}

// GetChannel gives request channel
func (n *SingleDataNode) GetChannel() chan DNRequest {
	return n.dataCh
}

// value + counters for a key
func (n *SingleDataNode) findSingleKey(key string) (any, int64, int64, bool) {

	n.Lock()
	defer n.Unlock()

	e, ok := n.dataMap[key]
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

	var needTouch []*list.Element // the records need to be refreshed
	for _, key := range keys {
		if e, ok := n.dataMap[key]; ok {
			e.Value.(*dataEntry).useCounterR += 1
			needTouch = append(needTouch, e)
			resKeys = append(resKeys, e.Value.(*dataEntry).key)
			resValues = append(resValues, e.Value.(*dataEntry).value)
		}
	}

	for _, e := range needTouch {
		n.data.MoveToFront(e)
	}
	return resKeys, resValues
}

// puts a new record or updates if exists. returns true if it's new, false if updated
// warning: not protected by a mutex
func (n *SingleDataNode) storeSingleRecord(key string, value any) bool {

	e, ok := n.dataMap[key]
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
		delete(n.dataMap, key)
	}
	e = n.data.PushFront(&dataEntry{ // make a new pair and push it as the most recent
		key:         key,
		value:       value,
		useCounterW: 1,
	})
	n.dataMap[key] = e
	return true
}

func (n *SingleDataNode) storeMultipleRecords(keys []string, values []any) error {

	if len(values) != len(keys) {
		return fmt.Errorf("bad keys/values/ndx array dimensions %d/%d", len(keys), len(values))
	}
	n.Lock()
	defer n.Unlock()

	for i, k := range keys {
		n.storeSingleRecord(k, values[i])
	}
	return nil
}

// deletes certain records
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

// kills all data, returns number of records deleted
func (n *SingleDataNode) deleteAllRecords() (count int) {
	n.Lock()
	defer n.Unlock()
	count = n.data.Len()
	n.data = list.New()
	return
}

// Len returns current data size
func (n *SingleDataNode) Len() int {
	n.Lock()
	defer n.Unlock()
	return n.data.Len()
}

// main loop receiving requests
func (n *SingleDataNode) mainLoop() {

	for {
		select {
		case <-n.ctx.Done(): // user cancellation
			return
		case rq := <-n.dataCh:
			if rq.Command == "del" {
				count := n.deleteAllRecords()
				rq.BackCh <- DNResponse{
					Status:  "OK",
					Count:   count,
					Message: fmt.Sprintf("deleted %d records", count),
				}

			} else if rq.Command == "put" {
				//fmt.Printf("putting %d records\n", len(rq.Keys))
				err := n.storeMultipleRecords(rq.Keys, rq.Values)
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
