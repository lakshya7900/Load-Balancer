package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Backend struct {
	URL               string
	Alive             bool
	ActiveConnections int
}

var backends = []Backend{
	{
		URL:               "http://localhost:8081",
		Alive:             true,
		ActiveConnections: 0,
	},
	{
		URL:               "http://localhost:8082",
		Alive:             true,
		ActiveConnections: 0,
	},
	{
		URL:               "http://localhost:8083",
		Alive:             true,
		ActiveConnections: 0,
	},
}

var currentBackend = 0
var backendMutex sync.Mutex

// func getNextBackend() (Backend, bool) {
// 	backendMutex.Lock()
// 	defer backendMutex.Unlock()

// 	for i := 0; i < len(backends); i++ {
// 		backend := backends[currentBackend]

// 		currentBackend = (currentBackend + 1) % len(backends)

// 		if backend.Alive {
// 			return backend, true
// 		}
// 	}

// 	return Backend{}, false
// }

func checkBackends(client *http.Client) {
	for i := range backends {
		url := backends[i].URL + "/health"
		alive := false

		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				alive = true
			}
		}

		setBackendAlive(i, alive)
	}
}

func setBackendAlive(index int, alive bool) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	oldState := backends[index].Alive
	backends[index].Alive = alive

	if oldState != alive {
		if alive {
			fmt.Println("Backend recovered:", backends[index].URL)
		} else {
			fmt.Println("Backend marked unhealthy:", backends[index].URL)
		}
	}
}

func startHealthChecker() {
	client := http.Client{Timeout: 1 * time.Second}

	for {
		checkBackends(&client)
		time.Sleep(2 * time.Second)
	}
}

func getLeastConnectionsBackend() (int, Backend, bool) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	selectedIndex := -1

	for i := range backends {
		if !backends[i].Alive {
			continue
		}

		if selectedIndex == -1 || backends[i].ActiveConnections < backends[selectedIndex].ActiveConnections {
			selectedIndex = i
		}
	}

	if selectedIndex == -1 {
		return -1, Backend{}, false
	}

	backends[selectedIndex].ActiveConnections++

	fmt.Printf(
		"Selected %s | active connections: %d\n",
		backends[selectedIndex].URL,
		backends[selectedIndex].ActiveConnections,
	)

	return selectedIndex, backends[selectedIndex], true
}

func releaseBackend(index int) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	backends[index].ActiveConnections--

	fmt.Printf(
		"Released %s | active connections: %d\n",
		backends[index].URL,
		backends[index].ActiveConnections,
	)
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Load balancer recieved: ", r.Method, r.URL.Path)

	backendIndex, backend, ok := getLeastConnectionsBackend()
	if !ok {
		http.Error(w, "No healthy backend available", http.StatusServiceUnavailable)
		return
	}
	defer releaseBackend(backendIndex)

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
	go startHealthChecker()

	http.HandleFunc("/", proxyHandler)

	fmt.Println("Load balancer running on http://localhost:8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
