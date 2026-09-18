package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Backend struct {
	URL               string
	Alive             bool
	ActiveConnections int

	FailureCount	int
	CircuitState	CircuitState
	OpenedAt		time.Time

	HalfOpenProbeInFlight	bool
}

var backends = []Backend{
	{
		URL:               	"http://localhost:8081",
		Alive:             	true,
		ActiveConnections: 	0,
		CircuitState: 		CircuitClosed,	
	},
	{
		URL:               	"http://localhost:8082",
		Alive:             	true,
		ActiveConnections: 	0,
		CircuitState: 		CircuitClosed,	
	},
	{
		URL:               	"http://localhost:8083",
		Alive:             	true,
		ActiveConnections: 	0,
		CircuitState: 		CircuitClosed,
	},
}

var currentBackend = 0
var backendMutex sync.Mutex

type CircuitState int
const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

const failureThreshold = 3
const circuitCooldown = 5 * time.Second

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

var proxyTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout: 500 * time.Millisecond,
	}).DialContext,

	ResponseHeaderTimeout: 3 * time.Second,
	IdleConnTimeout:       30 * time.Second,
}

var proxyClient = &http.Client{
	Transport: proxyTransport,
	Timeout:   5 * time.Second,
}

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

func getLeastConnectionsBackend(excluded map[int]bool) (int, Backend, bool) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	selectedIndex := -1

	now := time.Now()

	for i := range backends {
		backend := &backends[i]

		if excluded[i] {
			continue
		}

		if !backend.Alive {
			continue
		}

		if backend.CircuitState == CircuitOpen {
			if now.Sub(backend.OpenedAt) < circuitCooldown {
				continue
			}

			backend.CircuitState = CircuitHalfOpen
			backend.HalfOpenProbeInFlight = false

			fmt.Println("Circuit moved to HALF_OPEN:", backend.URL)
		}

		if backend.CircuitState == CircuitHalfOpen && backend.HalfOpenProbeInFlight {
			continue 
		}

		if selectedIndex == -1 || backend.ActiveConnections < backends[selectedIndex].ActiveConnections {
			selectedIndex = i
		}
	}

	if selectedIndex == -1 {
		return -1, Backend{}, false
	}

	selected := &backends[selectedIndex]

	selected.ActiveConnections++
	if selected.CircuitState == CircuitHalfOpen {
		selected.HalfOpenProbeInFlight = true
	}

	fmt.Printf(
		"Selected %s | active connections: %d\n",
		selected.URL,
		selected.ActiveConnections,
	)

	return selectedIndex, *selected, true
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

func recordBackendFailure(index int) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	backend := &backends[index]

	backend.FailureCount++

	fmt.Printf("Backend %s failure count: %d\n", backend.URL, backend.FailureCount)

	if backend.CircuitState == CircuitHalfOpen {
		backend.CircuitState = CircuitOpen
		backend.OpenedAt = time.Now()
		backend.HalfOpenProbeInFlight = false

		fmt.Println("Circuit reopened:", backend.URL)
		return
	}

	if backend.FailureCount >= failureThreshold {
		backend.CircuitState = CircuitOpen
		backend.OpenedAt = time.Now()
		backend.HalfOpenProbeInFlight = false

		fmt.Println("circuit opened:", backend.URL)
	}
}

func recordBackendSuccess(index int) {
	backendMutex.Lock()
	defer backendMutex.Unlock()

	backend := &backends[index]

	backend.FailureCount = 0

	if backend.CircuitState == CircuitHalfOpen {
		backend.CircuitState = CircuitClosed
		backend.HalfOpenProbeInFlight = false

		fmt.Println("Circuit closed:", backend.URL)
	}
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Load balancer received:", r.Method, r.URL.RequestURI())

	maxAttempts := 1

	if r.Method == http.MethodGet {
		maxAttempts = 2
	}

	excluded := make(map[int]bool)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		backendIndex, backend, ok := getLeastConnectionsBackend(excluded)
		if !ok {
			return
		}

		backendURL := backend.URL + r.URL.RequestURI()

		proxyReq, err := http.NewRequest(
			r.Method,
			backendURL,
			nil,
		)
		if err != nil {
			http.Error(w, "Failed to create backend request", http.StatusInternalServerError)
			return
		}
		proxyReq.Header = r.Header.Clone()

		resp, err := proxyClient.Do(proxyReq)
		if err == nil {
			releaseBackend(backendIndex)
			recordBackendSuccess(backendIndex)

			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}

			w.WriteHeader(resp.StatusCode)

			_, copyErr := io.Copy(w, resp.Body)

			resp.Body.Close()

			if copyErr != nil {
				log.Println("failed to copy backend resposne:", copyErr)
			}

			return
		} else {
			releaseBackend(backendIndex)
			recordBackendFailure(backendIndex)
			excluded[backendIndex] = true

			log.Printf(
				"Attempt %d failed on %s: %v\n",
				attempt,
				backend.URL,
				err,
			)
		}

	}
	// backendIndex, backend, ok := getLeastConnectionsBackend()
	// if !ok {
	// 	http.Error(w, "No healthy backend available", http.StatusServiceUnavailable)
	// 	return
	// }
	// defer releaseBackend(backendIndex)

	// backendURL := backend.URL + r.URL.RequestURI()

	// fmt.Println("Routing", r.Method, r.URL.RequestURI(), "to", backend.URL)

	// proxyReq, err := http.NewRequest(
	// 	r.Method,
	// 	backendURL,
	// 	r.Body,
	// )
	// if err != nil {
	// 	http.Error(w, "Failed to create backend request", http.StatusInternalServerError)
	// 	return
	// }
	// proxyReq.Header = r.Header.Clone()

	// resp, err := proxyClient.Do(proxyReq)
	// if err != nil {
	// 	log.Println("Backend request failed:", backend.URL, "error:", err)
	// 	http.Error(w, "Backend request failed", http.StatusBadGateway)
	// 	return
	// }
	// defer resp.Body.Close()

	// for key, values := range resp.Header {
	// 	for _, value := range values {
	// 		w.Header().Add(key, value)
	// 	}
	// }

	// w.WriteHeader(resp.StatusCode)

	// _, err = io.Copy(w, resp.Body)
	// if err != nil {
	// 	log.Fatal("Failed to copy backend response: ", err)
	// }
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
