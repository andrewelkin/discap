package CacheManager

import (
	"errors"
	"fmt"
	"github.com/andrewelkin/discap/DataNode"
	"hash/maphash"
	"sync"
)

type DateNodesManager struct {
	nodes         []DataNode.SingleDataNode
	hash          maphash.Hash
	numberOfNodes uint64
}

func (m *DateNodesManager) New(numberOfNodes int) *DateNodesManager {

	m.numberOfNodes = uint64(numberOfNodes)
	m.nodes = make([]DataNode.SingleDataNode, numberOfNodes)
	return m
}

func (m *DateNodesManager) calcNodeIndex(key string) int {

	m.hash.Reset()
	m.hash.WriteString(key)
	sum := m.hash.Sum64()
	return int(sum % m.numberOfNodes)
}

type response struct {
}

func (m *DateNodesManager) HandleCacheRequest(command string, keys []string, values []string) any {

	switch command {

	case "del":
		count := 0
		for _, node := range m.nodes {
			count += node.Len()
			node.InvalidAll()
		}
		return map[string]any{
			"status":  "OK",
			"message": fmt.Sprintf("%d cache entries deleted", count),
		}

	case "get":
		if len(values) > 0 {
			return map[string]string{
				"status":  "Error",
				"message": "For a get request there should be no values,, only keys",
			}

		}
		if len(keys) == 0 { // status request
			var resp []string
			for i, n := range m.nodes {
				resp = append(resp, fmt.Sprintf("node %03d length %d", i, n.Len()))
			}
			return map[string]any{
				"status":  "OK",
				"message": resp,
			}
		}

		keyArrays := make([][]string, m.numberOfNodes)
		results := make([]map[string]any, m.numberOfNodes)
		for _, k := range keys {
			keyArrays[m.calcNodeIndex(k)] = append(keyArrays[m.calcNodeIndex(k)], k)
		}
		var wg sync.WaitGroup
		wg.Add(int(m.numberOfNodes))
		for i, node := range m.nodes {
			go func() {
				results[i] = node.FindMultipleKeysAr(keyArrays[i])
			}()
		}
		wg.Wait()
		var resp []string
		for _, r := range results {
			for k, v := range r {
				resp = append(resp, fmt.Sprintf("key '%s' value '%s'", k, v))
			}
		}
		return map[string]any{
			"status":  "OK",
			"message": resp,
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

		var errAll error
		for i, node := range m.nodes {
			err := node.MaybePushMultipleAr(keyArrays[i], valueArrays[i])
			if err != nil {
				errAll = errors.Join(errAll, err)
			}
		}
		if errAll != nil {
			return map[string]string{
				"status":  "Error",
				"message": errAll.Error(),
			}
		} else {
			return map[string]string{
				"status":  "OK",
				"message": fmt.Sprintf("%d key/value pairs are sent to the cache", len(keys)),
			}
		}
	}
	return map[string]string{
		"status":  "Error",
		"message": "Unknown request: " + command,
	}
}
