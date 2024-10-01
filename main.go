package main

import (
	"context"
	"fmt"
	"github.com/andrewelkin/discap/CacheManager"
	"github.com/andrewelkin/discap/DataNode"
	"github.com/andrewelkin/discap/SimpleWeb"
	"log"
	"os"
	"strconv"
	"strings"
)

// the main creates three components:
// * web server accepting GET POST and DELETE requests
// * array of data nodes, each of them has a channel to receive requests
// * cache manager which passes requests/responses between web server and the nodes

// the main accepts three parameters, the cmd line syntax is:
//  [-p=<port number>] [-s=<node size>] [-n=<number of nodes>]
// example:
//   go run main.go -p=8080 -s=2048 -n=42
// the defaults are 8089 , 50 and 3
//

func main() {

	numberOfNodes := 3
	nodeMaxSize := 50
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

	log.Printf("Cache manager and web server are starting on port %d, max size: %d, number of nodes: %d\n", port, nodeMaxSize, numberOfNodes)

	ctx, _ := context.WithCancel(context.Background())

	// create the data nodes and get their channels
	nodeChannels := make([]chan<- DataNode.DNRequest, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		nodeChannels[i] = (&DataNode.SingleDataNode{}).New(ctx, fmt.Sprintf("%03d", i), nodeMaxSize).GetChannel()
	}

	// create the cache manager and give him the channels of the nodes
	cacheManager := (&CacheManager.DateNodesManager{}).New(ctx, nodeChannels)

	// start the simplest web server and give him the Cache manager
	(&SimpleWeb.JustWebServer{}).StartAndServe(port, cacheManager)

}
