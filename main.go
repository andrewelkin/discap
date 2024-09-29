package main

import (
	"github.com/andrewelkin/discap/CacheManager"
	"github.com/andrewelkin/discap/SimpleWeb"
)

func main() {

	numberOfNodes := 3
	nodeMaxSize := 5
	port := 8089

	CacheManager := (&CacheManager.DateNodesManager{}).New(numberOfNodes, nodeMaxSize)
	(&SimpleWeb.JustWebServer{}).StartAndServe(port, CacheManager)

}
