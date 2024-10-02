package CacheManager

import (
	"context"
	"log"
	"sync/atomic"

	"fmt"
	"github.com/andrewelkin/discap/DataNode"
	"hash/maphash"
	"sync"
)

// DateNodesManager is a cache manager.
// It keeps two sets of channels, one to send requests to the nodes, the other to receive responses
type DateNodesManager struct {
	nodeCh        []chan<- DataNode.DNRequest // nodes channels
	nodeBackCh    []chan DataNode.DNResponse  // response channels
	hash          maphash.Hash                // 64 bit hasher
	numberOfNodes int
}

// New  constructs a new cache manager
// --> Input:
// ctx              context.Context                 execution context
// nodeChannels     []chan<- DataNode.DNRequest     fully initialized channels to send requests to the nodes. len() defines number of nodes available
// <-- Output:
// 1) *DateNodesManager     initialized cache manager
func (m *DateNodesManager) New(ctx context.Context, nodeChannels []chan<- DataNode.DNRequest) *DateNodesManager {

	m.nodeCh = nodeChannels
	m.numberOfNodes = len(nodeChannels)
	m.nodeBackCh = make([]chan DataNode.DNResponse, m.numberOfNodes)
	for i := 0; i < m.numberOfNodes; i++ {
		m.nodeBackCh[i] = make(chan DataNode.DNResponse, 100)
	}

	return m
}

// calculates number of a node by the key (reminder)
func (m *DateNodesManager) calcNodeIndex(key string) int {
	m.hash.Reset()
	_, _ = m.hash.WriteString(key)
	sum := m.hash.Sum64()
	return int(sum % (uint64(m.numberOfNodes)))
}

// HandleCacheRequest passes requests and responses to/from nodes to web server. Parallelized requests to the nodes
// --> Input:
// command     string       command, one of the "get" "put "del"
// keys        []string     array of keys
// values      []string     array of values (or empty if not a "put" command)
// <-- Output:
// 1) any     returns an object to be sent to the operator
func (m *DateNodesManager) HandleCacheRequest(command string, keys []string, values []string) any {

	switch command {

	case "del": // request to clear the cache

		count := 0
		var wg sync.WaitGroup

		for i := 0; i < m.numberOfNodes; i++ {

			wg.Add(1)
			go func(nodeCh chan<- DataNode.DNRequest, bkCh chan DataNode.DNResponse) {

				rq := DataNode.DNRequest{
					Command: "del",
					BackCh:  bkCh,
				}
				nodeCh <- rq   // send request to a node
				resp := <-bkCh // get the response
				count += resp.Count
				wg.Done()

			}(m.nodeCh[i], m.nodeBackCh[i])
		}

		wg.Wait()

		log.Printf("[CMg] Cache deleted")
		return map[string]any{
			"status":  "OK",
			"message": fmt.Sprintf("%d cache entries deleted", count),
		}

	case "get": // request to find the keys in the cache. special case: empty keys array: sends status info about the nodes
		if len(values) > 0 {
			return map[string]string{
				"status":  "Error",
				"message": "For a get request there should be no values, only keys",
			}

		}
		if len(keys) == 0 { // status request
			var results []string
			for i := 0; i < m.numberOfNodes; i++ {
				rq := DataNode.DNRequest{
					Command: "get",
					BackCh:  m.nodeBackCh[i],
				}
				m.nodeCh[i] <- rq         // send request
				resp := <-m.nodeBackCh[i] // get response
				results = append(results, fmt.Sprintf("node %03d length %d", i, resp.Count))
			}

			return map[string]any{
				"status":  "OK",
				"message": results,
			}
		}

		keyArrays := make([][]string, m.numberOfNodes)
		results := make([]map[string]any, m.numberOfNodes)

		for _, k := range keys {
			keyArrays[m.calcNodeIndex(k)] = append(keyArrays[m.calcNodeIndex(k)], k)
		}
		var wg sync.WaitGroup
		var count atomic.Int64

		for i := 0; i < m.numberOfNodes; i++ {
			results[i] = make(map[string]any)
			if len(keyArrays[i]) > 0 {

				wg.Add(1)
				go func(keyAr []string, nodeCh chan<- DataNode.DNRequest, bkCh chan DataNode.DNResponse, result map[string]any) {

					rq := DataNode.DNRequest{
						Command: "get",
						Keys:    keyAr,
						BackCh:  bkCh,
					}
					nodeCh <- rq   // send request to a node
					resp := <-bkCh // get the response
					for j, k := range resp.Keys {
						count.Add(1)
						result[k] = resp.Values[j]
					}
					wg.Done()

				}(keyArrays[i], m.nodeCh[i], m.nodeBackCh[i], results[i])

			}
		}
		wg.Wait()
		result := make(map[string]any)
		for i := 0; i < m.numberOfNodes; i++ {
			for k, v := range results[i] {
				result[k] = v
			}
		}

		log.Printf("[CMg] %d key/value pairs are retrieved from the cache", count.Load())
		return map[string]any{
			"status": "OK",
			"result": result,
		}

	case "put": // request to store/update the keys

		if len(values) != len(keys) || len(keys) == 0 {
			em := "For a put request there should be equal nonzero number of keys and values"
			log.Printf("[CMg] %s", em)

			return map[string]string{
				"status":  "Error",
				"message": em,
			}
		}

		keyArrays := make([][]string, m.numberOfNodes)
		valueArrays := make([][]any, m.numberOfNodes)

		for i, k := range keys {
			keyArrays[m.calcNodeIndex(k)] = append(keyArrays[m.calcNodeIndex(k)], k)
			valueArrays[m.calcNodeIndex(k)] = append(valueArrays[m.calcNodeIndex(k)], values[i])
		}

		var errMessages []string
		var results []string
		var wg sync.WaitGroup
		var count atomic.Int64
		for i := 0; i < m.numberOfNodes; i++ {
			if len(keyArrays[i]) > 0 {
				wg.Add(1)
				go func(keyAr []string, valAr []any, nodeCh chan<- DataNode.DNRequest, bkCh chan DataNode.DNResponse, ndx int) {

					rq := DataNode.DNRequest{
						Command: "put",
						Keys:    keyAr,
						Values:  valAr,
						BackCh:  bkCh,
					}

					nodeCh <- rq   // send request to a node
					resp := <-bkCh // get the response
					count.Add(int64(resp.Count))
					wg.Done()
					if resp.Status != "OK" {
						errMessages = append(errMessages, fmt.Sprintf("node %d error: %s", ndx, resp.Message))
					} else {
						results = append(results, fmt.Sprintf("node %d:  %s", ndx, resp.Message))
					}

				}(keyArrays[i], valueArrays[i], m.nodeCh[i], m.nodeBackCh[i], i)
			}
		}
		wg.Wait()

		if len(errMessages) != 0 {
			log.Printf("[CMg] error: %v ", errMessages)
			return map[string]any{
				"status":  "Error",
				"message": errMessages,
			}
		} else {
			log.Printf("[CMg] %d key/value pairs are sent to the cache", count.Load())
			return map[string]any{
				"status":  "OK",
				"message": fmt.Sprintf("%d key/value pairs are sent to the cache", count.Load()),
				"debug":   results,
			}
		}
	}
	return map[string]string{
		"status":  "Error",
		"message": "Unknown request: " + command,
	}
}
