package CacheManager

import (
	"context"

	"fmt"
	"github.com/andrewelkin/discap/DataNode"
	"hash/maphash"
	"sync"
)

type DateNodesManager struct {
	nodeCh        []chan DataNode.DNRequest
	nodeBackCh    []chan DataNode.DNResponse
	hash          maphash.Hash
	numberOfNodes int
}

func (m *DateNodesManager) New(ctx context.Context, nodeChannels []chan DataNode.DNRequest) *DateNodesManager {

	m.nodeCh = nodeChannels
	m.numberOfNodes = len(nodeChannels)
	m.nodeBackCh = make([]chan DataNode.DNResponse, m.numberOfNodes)
	for i := 0; i < m.numberOfNodes; i++ {
		m.nodeBackCh[i] = make(chan DataNode.DNResponse, 100)
	}

	return m
}

func (m *DateNodesManager) calcNodeIndex(key string) int {
	m.hash.Reset()
	_, _ = m.hash.WriteString(key)
	sum := m.hash.Sum64()
	return int(sum % (uint64(m.numberOfNodes)))
}

func (m *DateNodesManager) HandleCacheRequest(command string, keys []string, values []string) any {

	switch command {

	case "del":

		count := 0
		var wg sync.WaitGroup

		for i := 0; i < m.numberOfNodes; i++ {

			go func(nodeCh chan DataNode.DNRequest, bkCh chan DataNode.DNResponse) {
				wg.Add(1)
				rq := DataNode.DNRequest{
					Command: "get",
					BackCh:  bkCh,
				}
				nodeCh <- rq
				// wait the response and send it
				resp := <-bkCh
				count += resp.Count
				wg.Done()

			}(m.nodeCh[i], m.nodeBackCh[i])
		}

		wg.Wait()

		return map[string]any{
			"status":  "OK",
			"message": fmt.Sprintf("%d cache entries deleted", count),
		}

	case "get":
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
				m.nodeCh[i] <- rq
				resp := <-m.nodeBackCh[i]
				results = append(results, fmt.Sprintf("node %03d length %d", i, resp.Count))
			}

			return map[string]any{
				"status":  "OK",
				"message": results,
			}
		}

		keyArrays := make([][]string, m.numberOfNodes)
		result := make(map[string]any)
		for _, k := range keys {
			keyArrays[m.calcNodeIndex(k)] = append(keyArrays[m.calcNodeIndex(k)], k)
		}
		var wg sync.WaitGroup

		for i := 0; i < m.numberOfNodes; i++ {
			if len(keyArrays[i]) > 0 {
				go func(keyAr []string, nodeCh chan DataNode.DNRequest, bkCh chan DataNode.DNResponse) {

					wg.Add(1)
					rq := DataNode.DNRequest{
						Command: "get",
						Keys:    keyAr,
						BackCh:  bkCh,
					}
					nodeCh <- rq
					// wait for the response
					resp := <-bkCh
					for j, k := range resp.Keys {
						result[k] = resp.Values[j]
					}
					wg.Done()

				}(keyArrays[i], m.nodeCh[i], m.nodeBackCh[i])

			}
		}
		wg.Wait()
		return map[string]any{
			"status": "OK",
			"result": result,
		}

	case "put":

		if len(values) != len(keys) || len(keys) == 0 {
			return map[string]string{
				"status":  "Error",
				"message": "For a put request there should be equal nonzero number of keys and values",
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
		count := 0
		for i := 0; i < m.numberOfNodes; i++ {
			if len(keyArrays[i]) > 0 {
				go func(keyAr []string, valAr []any, nodeCh chan DataNode.DNRequest, bkCh chan DataNode.DNResponse, ndx int) {
					wg.Add(1)
					rq := DataNode.DNRequest{
						Command: "put",
						Keys:    keyAr,
						Values:  valAr,
						BackCh:  bkCh,
					}

					nodeCh <- rq
					resp := <-bkCh
					count += resp.Count
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
			return map[string]any{
				"status":  "Error",
				"message": errMessages,
			}
		} else {
			return map[string]any{
				"status":  "OK",
				"message": fmt.Sprintf("%d key/value pairs are sent to the cache", count),
				"debug":   results,
			}
		}
	}
	return map[string]string{
		"status":  "Error",
		"message": "Unknown request: " + command,
	}
}
