package SimpleWeb

import (
	"encoding/json"
	"fmt"
	"github.com/andrewelkin/discap/CacheManager"
	"io"
	"net/http"
	"os"
)

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
	io.WriteString(w, string(b))
}

func (s *JustWebServer) StartAndServe(port int, cacheManager *CacheManager.DateNodesManager) {

	s.cacheManager = cacheManager
	http.HandleFunc("/", s.justHandler)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}

}
