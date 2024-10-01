package CacheManager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/andrewelkin/discap/DataNode"
	"testing"
)

func TestDateNodesManager_HandleCacheRequest(t *testing.T) {

	numberOfNodes := 10
	nodeMaxSize := 3

	// prep
	ctx, _ := context.WithCancel(context.Background())

	// create the data nodes and get their channels
	nodeChannels := make([]chan<- DataNode.DNRequest, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		nodeChannels[i] = (&DataNode.SingleDataNode{}).New(ctx, fmt.Sprintf("%03d", i), nodeMaxSize).GetChannel()
	}
	// create the cache manager and give him the channels of the nodes
	m := (&DateNodesManager{}).New(ctx, nodeChannels)

	// test
	keys := []string{
		"key1",
		"key2",
		"key3",
	}
	values := []string{
		"value1",
		"value2",
		"value3",
	}
	resp := m.HandleCacheRequest("put", keys, values)

	r, _ := json.MarshalIndent(resp, "", "\t")
	if resp.(map[string]any)["status"].(string) != "OK" {
		t.Errorf("HandleCacheRequest put error, response was %v", string(r))

	}
	//fmt.Printf("%v", string(r))
	resp = m.HandleCacheRequest("get", keys, nil)
	r, _ = json.MarshalIndent(resp, "", "\t")
	if resp.(map[string]any)["status"].(string) != "OK" {
		t.Errorf("HandleCacheRequest put error, response was %v", string(r))
	}

	result := resp.(map[string]any)["result"].(map[string]any)

	for i, k := range keys {
		if result[k] != values[i] {
			t.Errorf("HandleCacheRequest error, for the key %s expected %v, got %v", k, values[i], result[k])
		}
	}
	//fmt.Printf("%v", result)
	//fmt.Printf("%v", string(r))
}
