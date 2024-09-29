package CacheManager

import (
	"errors"
	"fmt"
	"github.com/andrewelkin/discap/DataNode"
	"hash/maphash"
	"sync"
)

type DateNodesManager struct {
	nodes         []*DataNode.SingleDataNode
	hash          maphash.Hash
	numberOfNodes uint64
}

func (m *DateNodesManager) New(numberOfNodes int, maxsize int) *DateNodesManager {

	m.numberOfNodes = uint64(numberOfNodes)
	m.nodes = make([]*DataNode.SingleDataNode, numberOfNodes)
	for i := range m.nodes {

		m.nodes[i] = (&DataNode.SingleDataNode{}).New(maxsize)
	}
	return m
}

func (m *DateNodesManager) calcNodeIndex(key string) int {

	m.hash.Reset()
	_, _ = m.hash.WriteString(key)
	sum := m.hash.Sum64()
	return int(sum % m.numberOfNodes)
}

func (m *DateNodesManager) HandleCacheRequest(command string, keys []string, values []string) any {

	switch command {

	case "del":
		count := 0
		for i := range m.nodes {
			count += m.nodes[i].Len()
			m.nodes[i].InvalidAll()
		}
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
			var resp []string
			for i := range m.nodes {
				resp = append(resp, fmt.Sprintf("node %03d length %d", i, m.nodes[i].Len()))
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
		for i := range m.nodes {
			go func(nodeNdx int, node *DataNode.SingleDataNode, keyArr []string) {
				results[nodeNdx] = node.FindMultipleKeysAr(keyArr)
				wg.Done()
			}(i, m.nodes[i], keyArrays[i])
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
		for i := range m.nodes {
			err := m.nodes[i].MaybePushMultipleAr(keyArrays[i], valueArrays[i])
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
