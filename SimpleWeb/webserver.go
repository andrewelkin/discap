package SimpleWeb

import (
	"encoding/json"
	"fmt"
	"github.com/andrewelkin/discap/CacheManager"
	"io"
	"net/http"
	"os"
)

// JustWebServer is a primitive web server. it keeps a pointer to the cache manager and passes requests
type JustWebServer struct {
	cacheManager *CacheManager.DateNodesManager
}

func (s *JustWebServer) justHandler(w http.ResponseWriter, r *http.Request) {

	values := r.URL.Query()
	var resp any

	switch r.Method {
	case http.MethodPost:
		resp = s.cacheManager.HandleCacheRequest("put", values["key"], values["value"])
	case http.MethodGet:
		resp = s.cacheManager.HandleCacheRequest("get", values["key"], nil)
	case http.MethodDelete:
		resp = s.cacheManager.HandleCacheRequest("del", nil, nil)
	default:
		resp = map[string]any{
			"status":  "Error",
			"message": "Unknown request type, we support only POST GET and DELETE!",
		}
	}
	b, _ := json.Marshal(&resp)
	_, _ = io.WriteString(w, string(b))
}

// StartAndServe starts a simple web server. it passes requests to the cache manager
// --> Input:
// port             int                                port to listen, 8089 default
// cacheManager     *CacheManager.DateNodesManager     points to cache manager
func (s *JustWebServer) StartAndServe(port int, cacheManager *CacheManager.DateNodesManager) {

	s.cacheManager = cacheManager
	http.HandleFunc("/", s.justHandler)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}

}
