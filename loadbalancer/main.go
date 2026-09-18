package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Backend struct {
	URL               string
	Alive             bool
	ActiveConnections int

	FailureCount int
	CircuitState CircuitState
	OpenedAt     time.Time

	HalfOpenProbeInFlight bool
}

var backends = []Backend{
	{
		URL:               "http://localhost:8081",
		Alive:             true,
		ActiveConnections: 0,
		CircuitState:      CircuitClosed,
	},
	{
		URL:               "http://localhost:8082",
		Alive:             true,
		ActiveConnections: 0,
		CircuitState:      CircuitClosed,
	},
	{
		URL:               "http://localhost:8083",
		Alive:             true,
		ActiveConnections: 0,
		CircuitState:      CircuitClosed,
	},
}

type BackendPool struct {
	Name     string
	Backends []Backend
	mu       sync.Mutex
}

var userPool = &BackendPool{
	Name: "user",
	Backends: []Backend{
		{
			URL:               "http://localhost:8081",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
		{
			URL:               "http://localhost:8082",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
	},
}

var paymentsPool = &BackendPool{
	Name: "payments",
	Backends: []Backend{
		{
			URL:               "http://localhost:8083",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
		{
			URL:               "http://localhost:8084",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
	},
}

var staticPool = &BackendPool{
	Name: "static",
	Backends: []Backend{
		{
			URL:               "http://localhost:8085",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
		{
			URL:               "http://localhost:8086",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
	},
}

var userBetaPool = &BackendPool{
	Name: "users-beta",
	Backends: []Backend{
		{
			URL:               "http://localhost:8087",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
		{
			URL:               "http://localhost:8088",
			Alive:             true,
			ActiveConnections: 0,
			CircuitState:      CircuitClosed,
		},
	},
}

var allPools = []*BackendPool{
	userPool,
	paymentsPool,
	staticPool,
	userBetaPool,
}

type Route struct {
	Prefix       string
	DefaultPool  *BackendPool
	VersionPools map[string]*BackendPool
}

var routes = []Route{
	{
		Prefix:      "/api/users",
		DefaultPool: userPool,
		VersionPools: map[string]*BackendPool{
			"beta": userBetaPool,
		},
	},
	{
		Prefix:       "/api/payments",
		DefaultPool:  paymentsPool,
		VersionPools: nil,
	},
	{
		Prefix:       "/static",
		DefaultPool:  staticPool,
		VersionPools: nil,
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

type TokenBucket struct {
	Capacity   float64
	Tokens     float64
	RefillRate float64
	LastRefill time.Time

	mu sync.Mutex
}

var rateLimiter = TokenBucket{
	Capacity:   10,
	Tokens:     10,
	RefillRate: 2,
	LastRefill: time.Now(),
}

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
	DialContext: loggingDialContext,

	ResponseHeaderTimeout: 3 * time.Second,
	IdleConnTimeout:       30 * time.Second,

	MaxIdleConns:			100,
	MaxIdleConnsPerHost:	20,
	MaxConnsPerHost: 		50,
}

var proxyClient = &http.Client{
	Transport: proxyTransport,
	Timeout:   5 * time.Second,
}

var dialer = &net.Dialer{
	Timeout: 500 * time.Millisecond,
}

func loggingDialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	fmt.Println("Creating new TCP connection to", address)
	return  dialer.DialContext(ctx, network, address)
}

func checkBackends(client *http.Client) {
	for _, pool := range allPools {
		checkPoolBackend(pool, client)
	}
}

func checkPoolBackend(pool *BackendPool, client *http.Client) {
	for i := range pool.Backends {
		url := pool.Backends[i].URL + "/health"
		alive := false

		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				alive = true
			}
		}

		pool.setBackendAlive(i, alive)
	}
}

func (pool *BackendPool) setBackendAlive(index int, alive bool) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	oldState := pool.Backends[index].Alive
	pool.Backends[index].Alive = alive

	if oldState != alive {
		if alive {
			fmt.Println("Backend recovered:", pool.Backends[index].URL)
		} else {
			fmt.Println("Backend marked unhealthy:", pool.Backends[index].URL)
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

func (pool *BackendPool) getLeastConnectionsBackend(excluded map[int]bool) (int, Backend, bool) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	selectedIndex := -1

	now := time.Now()

	for i := range pool.Backends {
		backend := &pool.Backends[i]

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

		if selectedIndex == -1 || backend.ActiveConnections < pool.Backends[selectedIndex].ActiveConnections {
			selectedIndex = i
		}
	}

	if selectedIndex == -1 {
		return -1, Backend{}, false
	}

	selected := &pool.Backends[selectedIndex]

	selected.ActiveConnections++
	if selected.CircuitState == CircuitHalfOpen {
		selected.HalfOpenProbeInFlight = true
	}

	fmt.Printf(
		"Pool=%s | Selected=%s | Active Connections=%d\n",
		pool.Name,
		selected.URL,
		selected.ActiveConnections,
	)

	return selectedIndex, *selected, true
}

func (pool *BackendPool) releaseBackend(index int) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.Backends[index].ActiveConnections--

	fmt.Printf(
		"Pool=%s | Released=%s | Active Connections=%d\n",
		pool.Name,
		pool.Backends[index].URL,
		pool.Backends[index].ActiveConnections,
	)
}

func (pool *BackendPool) recordBackendFailure(index int) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	backend := &pool.Backends[index]

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

func (pool *BackendPool) recordBackendSuccess(index int) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	backend := &pool.Backends[index]

	backend.FailureCount = 0

	if backend.CircuitState == CircuitHalfOpen {
		backend.CircuitState = CircuitClosed
		backend.HalfOpenProbeInFlight = false

		fmt.Println("Circuit closed:", backend.URL)
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()

	elapsed := now.Sub(tb.LastRefill).Seconds()

	fmt.Printf(
		"Before refill: tokens=%.2f elapsed=%.2fs adding=%.2f\n",
		tb.Tokens,
		elapsed,
		elapsed*tb.RefillRate,
	)

	tb.Tokens += elapsed * tb.RefillRate

	if tb.Tokens > tb.Capacity {
		tb.Tokens = tb.Capacity
	}

	tb.LastRefill = now

	fmt.Printf("After refill: tokens=%.2f\n", tb.Tokens)

	if tb.Tokens < 1 {
		return false
	}

	tb.Tokens--

	return true
}

func getPoolForRequest(r *http.Request) (*BackendPool, bool) {
	for _, route := range routes {
		pathMatches := r.URL.Path == route.Prefix || strings.HasPrefix(r.URL.Path, route.Prefix+"/")
		if !pathMatches {
			continue
		}

		version := r.Header.Get("X-Version")

		if route.VersionPools != nil {
			if pool, exists := route.VersionPools[version]; exists {
				return pool, true
			}
		}

		return route.DefaultPool, true
	}

	return nil, false
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if !rateLimiter.Allow() {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	pool, ok := getPoolForRequest(r)
	if !ok {
		http.Error(w, "No route configured for this path", http.StatusNotFound)
		return
	}

	fmt.Println("Load balancer received:", r.Method, r.URL.RequestURI())

	maxAttempts := 1

	if r.Method == http.MethodGet {
		maxAttempts = 2
	}

	excluded := make(map[int]bool)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		backendIndex, backend, ok := pool.getLeastConnectionsBackend(excluded)
		if !ok {
			http.Error(w, "All backend attempts failed", http.StatusBadGateway)
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
			pool.releaseBackend(backendIndex)
			pool.recordBackendSuccess(backendIndex)

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
			pool.releaseBackend(backendIndex)
			pool.recordBackendFailure(backendIndex)
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
