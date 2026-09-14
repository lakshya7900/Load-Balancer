package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

var backends = []string{
	"http://localhost:8081",
	"http://localhost:8082",
	"http://localhost:8083",
}

var currentBackend = 0
var backendMutex sync.Mutex

func getNextBackend() string {
	backendMutex.Lock()
	defer backendMutex.Unlock()
	
	backend := backends[currentBackend]

	// TEMP: force a data race
	// time.Sleep(10 * time.Millisecond)

	currentBackend = (currentBackend + 1) % len(backends)
	return backend
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Load balancer recieved: ", r.Method, r.URL.Path)

	backend := getNextBackend()
	backendURL := backend + r.URL.RequestURI()

	fmt.Println("Routing", r.Method, r.URL.RequestURI(), "to", backend)

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
