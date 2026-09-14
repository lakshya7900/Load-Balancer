package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

type Backend struct {
	URL		string
	Alive	bool
}

var backends = []Backend{
	{
		URL: 	"http://localhost:8081",
		Alive: 	true,
	},
	{
		URL: 	"http://localhost:8082",
		Alive: 	true,
	},
	{
		URL: 	"http://localhost:8083",
		Alive: 	true,
	},
}

var currentBackend = 0
var backendMutex sync.Mutex

func getNextBackend() (Backend, bool) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	for i := 0; i < len(backends); i++ {
		backend := backends[currentBackend]
		
		currentBackend = (currentBackend + 1) % len(backends)

		if backend.Alive {
			return backend, true
		}
	}
	
	return Backend{}, false
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Load balancer recieved: ", r.Method, r.URL.Path)

	backend, ok := getNextBackend()
	if !ok {
		http.Error(w, "No healthy backend available", http.StatusServiceUnavailable)
		return
	}

	backendURL := backend.URL + r.URL.RequestURI()

	fmt.Println("Routing", r.Method, r.URL.RequestURI(), "to", backend.URL)

	proxyReq, err := http.NewRequest(
		r.Method,
		backendURL,
		r.Body,
	)
	if err != nil {
		http.Error(w, "Failed to create backend request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header.Clone()

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "Backend unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Fatal("Failed to copy backend response: ", err)
	}
}

func main() {
	http.HandleFunc("/hello", proxyHandler)

	fmt.Println("Load balancer running on http://localhost:8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
