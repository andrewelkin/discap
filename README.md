
## Distributed Cache Project


### What is this?

This project implements distributed cache with LRU eviction policy for each node.

Data nodes are receiving requests and sending responses though go channels (imitating pubsub environment).

Cache manager evenly distributes store requests among the nodes and orchestrates parallel retrieval of multiple records.

Web server simply passes requests and responses to/from the cache manager.

### Files in the repository

```

├── CacheManager
│   ├── cachemanager.go           <- cache manager implementation
│   └── cachemanager_test.go      <- unit tests
├── curl-tests.sh                       <- curl tests, (make it chmod +x curl-tests.sh)
├── DataNode
│   ├── datanode.go               <- data node implementation    
│   └── datanode_test.go          <- unit tests  
├── go.mod
├── LICENSE
├── main.go                             <- main file
├── README.md                           <- this file
├── SimpleWeb
    └── webserver.go              <- primitive web server


```



### How to work with it


It has a simple web server supporting three requests:


POST to store key/value pairs

GET to retrieve pairs

DELETE to clear the cache


Examples of the requests:

#### 'Storing 2 records:'
```
'POST'  'http://localhost:8089?key=key1&value=value1&key=key2&value=value2' 
```
response:
```
{
  "debug": [
    "node 1:  stored 1 records",
    "node 0:  stored 1 records"
  ],
  "message": "2 key/value pairs are sent to the cache",
  "status": "OK"
}
```

#### 'Getting records:'
```
'GET'  'http://localhost:8089?key=key1&key=key2&key=key3&key=key4' 
```

response:
```
{
  "result": {
    "key1": "value1",
    "key2": "value2",
  },
  "status": "OK"
}
```

Note that request 
```
'GET'  'http://localhost:8089'
```
will return info about current state of the nodes, response:

```
{
  "message": [
    "node 000 length 1",
    "node 001 length 1"
  ],
  "status": "OK"
}
```

#### 'Deleting cache:'
```
'DELETE' 'http://localhost:8089'
```

### How to run

After cloning the repository, from the project root directory:

example:
`go run main.go -p=8080 -s=2048 -n=42`

The cmd line syntax is:
`[-p=<port number>] [-s=<node size>] [-n=<number of nodes>]`

the defaults are 8089 , 50 and 3

### How to test


for unit tests: 

`go test ./...`

for full functional test please refer to curl-tests.sh


