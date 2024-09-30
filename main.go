package main

import (
	"context"
	"fmt"
	"github.com/andrewelkin/discap/CacheManager"
	"github.com/andrewelkin/discap/DataNode"
	"github.com/andrewelkin/discap/SimpleWeb"
	"os"
	"strconv"
	"strings"
)

func main() {

	numberOfNodes := 3
	nodeMaxSize := 5
	port := 8089

	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "-p=") {
			if tmp, err := strconv.ParseInt(a[3:], 10, 64); err == nil {
				port = int(tmp)
			}
		}
		if strings.HasPrefix(a, "-s=") {
			if tmp, err := strconv.ParseInt(a[3:], 10, 64); err == nil {
				nodeMaxSize = int(tmp)
			}
		}
		if strings.HasPrefix(a, "-n=") {
			if tmp, err := strconv.ParseInt(a[3:], 10, 64); err == nil {
				numberOfNodes = int(tmp)
			}
		}

	}

	fmt.Printf("\nCache manager and web server are starting on port %d, max size: %d, number of nodes: %d\n", port, nodeMaxSize, numberOfNodes)

	ctx, _ := context.WithCancel(context.Background())

	// create the data nodes and get their channels
	nodeChannels := make([]chan DataNode.DNRequest, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		nodeChannels[i] = (&DataNode.SingleDataNode{}).New(ctx, nodeMaxSize).GetChannel()
	}

	// create the cache manager and give him the channels of the nodes
	CacheManager := (&CacheManager.DateNodesManager{}).New(ctx, nodeChannels)

	// start the simplest web server and give him the Cache manager
	(&SimpleWeb.JustWebServer{}).StartAndServe(port, CacheManager)

}
