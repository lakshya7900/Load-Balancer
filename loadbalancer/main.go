package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type CircuitState int32

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

const (
	failureThreshold       = 3
	circuitCooldown        = 5 * time.Second
	healthCheckEvery       = 2 * time.Second
	healthCheckTimeout     = 2 * time.Second
	healthFailureThreshold = 3
	cacheTTL               = 10 * time.Second
	maxCacheBodySize       = 1 << 20 // 1 MiB
	latencySampleLimit     = 10000
)

type Backend struct {
	URL string

	Alive                 atomic.Bool
	ActiveConnections     atomic.Int64
	FailureCount          atomic.Int64
	CircuitState          atomic.Int32
	OpenedAtUnixNano      atomic.Int64
	HalfOpenProbeInFlight atomic.Bool
	HealthFailures        atomic.Int32
	Requests              atomic.Uint64
}

func newBackend(url string) *Backend {
	b := &Backend{URL: url}
	b.Alive.Store(true)
	b.CircuitState.Store(int32(CircuitClosed))
	return b
}

type BackendPool struct {
	Name     string
	Backends []*Backend

	Requests   atomic.Uint64
	tieBreaker atomic.Uint64
}

var userPool = &BackendPool{
	Name: "users",
	Backends: []*Backend{
		newBackend("http://127.0.0.1:8081"),
		newBackend("http://127.0.0.1:8082"),
	},
}

var paymentsPool = &BackendPool{
	Name: "payments",
	Backends: []*Backend{
		newBackend("http://127.0.0.1:8083"),
		newBackend("http://127.0.0.1:8084"),
	},
}

var staticPool = &BackendPool{
	Name: "static",
	Backends: []*Backend{
		newBackend("http://127.0.0.1:8085"),
		newBackend("http://127.0.0.1:8086"),
	},
}

var userBetaPool = &BackendPool{
	Name: "users-beta",
	Backends: []*Backend{
		newBackend("http://127.0.0.1:8087"),
		newBackend("http://127.0.0.1:8088"),
	},
}

var allPools = []*BackendPool{userPool, paymentsPool, staticPool, userBetaPool}

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
	{Prefix: "/api/payments", DefaultPool: paymentsPool},
	{Prefix: "/static", DefaultPool: staticPool},
}

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

// -------------------- Backend HTTP client / connection pool --------------------

var dialer = &net.Dialer{
	Timeout:   500 * time.Millisecond,
	KeepAlive: 30 * time.Second,
}

func loggingDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if os.Getenv("LOG_DIALS") == "1" {
		log.Println("creating new TCP connection to", address)
	}
	return dialer.DialContext(ctx, network, address)
}

var proxyTransport = &http.Transport{
	DialContext:           loggingDialContext,
	ResponseHeaderTimeout: 3 * time.Second,
	IdleConnTimeout:       60 * time.Second,
	MaxIdleConns:          2048,
	MaxIdleConnsPerHost:   256,
	MaxConnsPerHost:       256,
	ForceAttemptHTTP2:     false,
}

var proxyClient = &http.Client{
	Transport: proxyTransport,
	Timeout:   5 * time.Second,
}

// -------------------- Cache --------------------

type CacheEntry struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	ExpiresAt  time.Time
}

type ResponseCache struct {
	entries map[string]CacheEntry
	mu      sync.RWMutex
}

var responseCache = ResponseCache{entries: make(map[string]CacheEntry)}

func (c *ResponseCache) Get(key string) (CacheEntry, bool) {
	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		return CacheEntry{}, false
	}

	if time.Now().After(entry.ExpiresAt) {
		c.mu.Lock()
		current, stillExists := c.entries[key]
		if stillExists && time.Now().After(current.ExpiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return CacheEntry{}, false
	}

	return entry, true
}

func (c *ResponseCache) Set(key string, entry CacheEntry) {
	c.mu.Lock()
	c.entries[key] = entry
	c.mu.Unlock()
}

func (c *ResponseCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func getCacheKey(r *http.Request) string {
	return fmt.Sprintf(
		"%s|%s|version=%s|accept=%s|encoding=%s",
		r.Method,
		r.URL.RequestURI(),
		r.Header.Get("X-Version"),
		r.Header.Get("Accept"),
		r.Header.Get("Accept-Encoding"),
	)
}

func isCacheableRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && r.Header.Get("Authorization") == ""
}

// -------------------- Per-client token bucket --------------------

type TokenBucket struct {
	Capacity   float64
	Tokens     float64
	RefillRate float64
	LastRefill time.Time
	mu         sync.Mutex
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.LastRefill).Seconds()
	tb.Tokens += elapsed * tb.RefillRate
	if tb.Tokens > tb.Capacity {
		tb.Tokens = tb.Capacity
	}
	tb.LastRefill = now

	if tb.Tokens < 1 {
		return false
	}

	tb.Tokens--
	return true
}

type clientBucketEntry struct {
	Bucket   *TokenBucket
	LastSeen time.Time
}

type ClientRateLimiter struct {
	mu      sync.Mutex
	clients map[string]*clientBucketEntry
}

var clientRateLimiter = ClientRateLimiter{clients: make(map[string]*clientBucketEntry)}

func (rl *ClientRateLimiter) get(clientID string) *TokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	if entry, ok := rl.clients[clientID]; ok {
		entry.LastSeen = now
		return entry.Bucket
	}

	bucket := &TokenBucket{
		Capacity:   10,
		Tokens:     10,
		RefillRate: 2,
		LastRefill: now,
	}

	rl.clients[clientID] = &clientBucketEntry{Bucket: bucket, LastSeen: now}
	return bucket
}

func (rl *ClientRateLimiter) cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			rl.mu.Lock()
			for id, entry := range rl.clients {
				if now.Sub(entry.LastSeen) > 10*time.Minute {
					delete(rl.clients, id)
				}
			}
			rl.mu.Unlock()
		}
	}
}

func (rl *ClientRateLimiter) Len() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.clients)
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// -------------------- Lock-free hot-path metrics --------------------

type Metrics struct {
	RequestsTotal   atomic.Uint64
	ActiveRequests  atomic.Int64
	CacheHits       atomic.Uint64
	CacheMisses     atomic.Uint64
	RateLimited     atomic.Uint64
	Retries         atomic.Uint64
	BackendFailures atomic.Uint64

	// HTTP status codes are bounded, so avoid a map+mutex on the request path.
	StatusCodes [600]atomic.Uint64

	// Fixed-size circular latency sample. Each request does one atomic Add and
	// one atomic Store rather than shifting a 10k-element slice.
	LatencyWriteIndex atomic.Uint64
	Latencies         [latencySampleLimit]atomic.Int64
}

var metrics Metrics

func (m *Metrics) BeginRequest() {
	m.ActiveRequests.Add(1)
}

func (m *Metrics) EndRequest(status int, latency time.Duration) {
	m.ActiveRequests.Add(-1)
	m.RequestsTotal.Add(1)

	if status >= 0 && status < len(m.StatusCodes) {
		m.StatusCodes[status].Add(1)
	}

	slot := (m.LatencyWriteIndex.Add(1) - 1) % latencySampleLimit
	m.Latencies[slot].Store(latency.Nanoseconds())
}

func (m *Metrics) LatencySnapshot() []time.Duration {
	writes := m.LatencyWriteIndex.Load()
	count := writes
	if count > latencySampleLimit {
		count = latencySampleLimit
	}

	values := make([]time.Duration, 0, count)
	for i := uint64(0); i < count; i++ {
		ns := m.Latencies[i].Load()
		if ns > 0 {
			values = append(values, time.Duration(ns))
		}
	}
	return values
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}

	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}

	return r.ResponseWriter.Write(body)
}

func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}

	// Preserve net/http's optimized path when available.
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}

	bufPtr := copyBufferPool.Get().(*[]byte)

	n, err := io.CopyBuffer(
		r.ResponseWriter,
		src,
		*bufPtr,
	)

	copyBufferPool.Put(bufPtr)

	return n, err
}

func percentile(values []time.Duration, p float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * p)
	return float64(values[index]) / float64(time.Millisecond)
}

func metricLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	latencies := metrics.LatencySnapshot()
	p50 := percentile(append([]time.Duration(nil), latencies...), 0.50)
	p95 := percentile(append([]time.Duration(nil), latencies...), 0.95)
	p99 := percentile(append([]time.Duration(nil), latencies...), 0.99)

	cacheHits := metrics.CacheHits.Load()
	cacheMisses := metrics.CacheMisses.Load()
	cacheTotal := cacheHits + cacheMisses
	cacheHitRatio := 0.0
	if cacheTotal > 0 {
		cacheHitRatio = float64(cacheHits) / float64(cacheTotal)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "l7lb_requests_total %d\n", metrics.RequestsTotal.Load())
	fmt.Fprintf(w, "l7lb_active_requests %d\n", metrics.ActiveRequests.Load())
	fmt.Fprintf(w, "l7lb_cache_hits_total %d\n", cacheHits)
	fmt.Fprintf(w, "l7lb_cache_misses_total %d\n", cacheMisses)
	fmt.Fprintf(w, "l7lb_cache_hit_ratio %.6f\n", cacheHitRatio)
	fmt.Fprintf(w, "l7lb_cache_entries %d\n", responseCache.Len())
	fmt.Fprintf(w, "l7lb_rate_limited_total %d\n", metrics.RateLimited.Load())
	fmt.Fprintf(w, "l7lb_rate_limiter_clients %d\n", clientRateLimiter.Len())
	fmt.Fprintf(w, "l7lb_retries_total %d\n", metrics.Retries.Load())
	fmt.Fprintf(w, "l7lb_backend_failures_total %d\n", metrics.BackendFailures.Load())
	fmt.Fprintf(w, "l7lb_request_latency_p50_ms %.3f\n", p50)
	fmt.Fprintf(w, "l7lb_request_latency_p95_ms %.3f\n", p95)
	fmt.Fprintf(w, "l7lb_request_latency_p99_ms %.3f\n", p99)

	for status := 100; status < len(metrics.StatusCodes); status++ {
		count := metrics.StatusCodes[status].Load()
		if count > 0 {
			fmt.Fprintf(w, "l7lb_responses_total{status=\"%d\"} %d\n", status, count)
		}
	}

	for _, pool := range allPools {
		fmt.Fprintf(w, "l7lb_pool_requests_total{pool=\"%s\"} %d\n", metricLabel(pool.Name), pool.Requests.Load())

		for _, backend := range pool.Backends {
			alive := 0
			if backend.Alive.Load() {
				alive = 1
			}

			fmt.Fprintf(w,
				"l7lb_backend_requests_total{pool=\"%s\",backend=\"%s\"} %d\n",
				metricLabel(pool.Name), metricLabel(backend.URL), backend.Requests.Load(),
			)
			fmt.Fprintf(w,
				"l7lb_backend_alive{pool=\"%s\",backend=\"%s\"} %d\n",
				metricLabel(pool.Name), metricLabel(backend.URL), alive,
			)
			fmt.Fprintf(w,
				"l7lb_backend_active_connections{pool=\"%s\",backend=\"%s\"} %d\n",
				metricLabel(pool.Name), metricLabel(backend.URL), backend.ActiveConnections.Load(),
			)
			fmt.Fprintf(w,
				"l7lb_backend_failure_count{pool=\"%s\",backend=\"%s\"} %d\n",
				metricLabel(pool.Name), metricLabel(backend.URL), backend.FailureCount.Load(),
			)
			fmt.Fprintf(w,
				"l7lb_backend_circuit_state{pool=\"%s\",backend=\"%s\"} %d\n",
				metricLabel(pool.Name), metricLabel(backend.URL), backend.CircuitState.Load(),
			)
		}
	}
}

// -------------------- Health checks / backend state --------------------

func (b *Backend) recordHealth(healthy bool) {
	if healthy {
		b.HealthFailures.Store(0)
		if b.Alive.CompareAndSwap(false, true) {
			log.Println("backend recovered:", b.URL)
		}
		return
	}

	failures := b.HealthFailures.Add(1)
	if failures >= healthFailureThreshold && b.Alive.CompareAndSwap(true, false) {
		log.Println("backend marked unhealthy:", b.URL)
	}
}

func checkBackends(client *http.Client) {
	var wg sync.WaitGroup

	for _, pool := range allPools {
		for _, backend := range pool.Backends {
			wg.Add(1)
			go func(backend *Backend) {
				defer wg.Done()

				resp, err := client.Get(backend.URL + "/health")
				healthy := false

				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					healthy = resp.StatusCode == http.StatusOK
				}

				backend.recordHealth(healthy)
			}(backend)
		}
	}

	wg.Wait()
}

func startHealthChecker(ctx context.Context) {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Timeout:   healthCheckTimeout,
		Transport: transport,
	}

	checkBackends(client)

	ticker := time.NewTicker(healthCheckEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkBackends(client)
		}
	}
}

func (pool *BackendPool) getLeastConnectionsBackend(excluded map[int]bool) (int, *Backend, bool) {
	n := len(pool.Backends)
	if n == 0 {
		return -1, nil, false
	}

	// Rotating start position prevents permanent index-0 preference when
	// multiple backends have the same active count.
	start := int((pool.tieBreaker.Add(1) - 1) % uint64(n))

	for selectionAttempt := 0; selectionAttempt < n+1; selectionAttempt++ {
		selectedIndex := -1
		var selectedActive int64
		now := time.Now()

		for offset := 0; offset < n; offset++ {
			i := (start + offset) % n
			if excluded[i] {
				continue
			}

			backend := pool.Backends[i]
			if !backend.Alive.Load() {
				continue
			}

			state := CircuitState(backend.CircuitState.Load())
			if state == CircuitOpen {
				openedAtNS := backend.OpenedAtUnixNano.Load()
				if openedAtNS != 0 && now.Sub(time.Unix(0, openedAtNS)) < circuitCooldown {
					continue
				}

				if backend.CircuitState.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
					backend.HalfOpenProbeInFlight.Store(false)
					log.Println("circuit moved to HALF_OPEN:", backend.URL)
					state = CircuitHalfOpen
				} else {
					state = CircuitState(backend.CircuitState.Load())
				}
			}

			if state == CircuitHalfOpen && backend.HalfOpenProbeInFlight.Load() {
				continue
			}

			active := backend.ActiveConnections.Load()
			if selectedIndex == -1 || active < selectedActive {
				selectedIndex = i
				selectedActive = active
			}
		}

		if selectedIndex == -1 {
			return -1, nil, false
		}

		selected := pool.Backends[selectedIndex]

		// Only one request may become the HALF_OPEN probe.
		if CircuitState(selected.CircuitState.Load()) == CircuitHalfOpen {
			if !selected.HalfOpenProbeInFlight.CompareAndSwap(false, true) {
				start = (selectedIndex + 1) % n
				continue
			}
		}

		selected.ActiveConnections.Add(1)

		// State could have changed after selection. If it became unusable,
		// undo the reservation and try another backend.
		if !selected.Alive.Load() || CircuitState(selected.CircuitState.Load()) == CircuitOpen {
			selected.ActiveConnections.Add(-1)
			selected.HalfOpenProbeInFlight.Store(false)
			start = (selectedIndex + 1) % n
			continue
		}

		return selectedIndex, selected, true
	}

	return -1, nil, false
}

func (pool *BackendPool) releaseBackend(index int) {
	pool.Backends[index].ActiveConnections.Add(-1)
}

func (pool *BackendPool) recordBackendFailure(index int) {
	backend := pool.Backends[index]
	failures := backend.FailureCount.Add(1)
	state := CircuitState(backend.CircuitState.Load())

	if state == CircuitHalfOpen {
		backend.OpenedAtUnixNano.Store(time.Now().UnixNano())
		if backend.CircuitState.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitOpen)) {
			log.Println("circuit reopened:", backend.URL)
		}
		backend.HalfOpenProbeInFlight.Store(false)
		return
	}

	if failures >= failureThreshold && state == CircuitClosed {
		backend.OpenedAtUnixNano.Store(time.Now().UnixNano())
		if backend.CircuitState.CompareAndSwap(int32(CircuitClosed), int32(CircuitOpen)) {
			log.Println("circuit opened:", backend.URL)
		}
	}
}

func (pool *BackendPool) recordBackendSuccess(index int) {
	backend := pool.Backends[index]
	backend.FailureCount.Store(0)

	if backend.CircuitState.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitClosed)) {
		backend.HalfOpenProbeInFlight.Store(false)
		log.Println("circuit closed:", backend.URL)
	}
}

// -------------------- L7 routing / proxy helpers --------------------

func getPoolForRequest(r *http.Request) (*BackendPool, bool) {
	for _, route := range routes {
		matches := r.URL.Path == route.Prefix || strings.HasPrefix(r.URL.Path, route.Prefix+"/")
		if !matches {
			continue
		}

		version := r.Header.Get("X-Version")
		if pool, exists := route.VersionPools[version]; exists {
			return pool, true
		}

		return route.DefaultPool, true
	}

	return nil, false
}

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func removeHopByHopHeaders(header http.Header) {
	if connection := header.Get("Connection"); connection != "" {
		for _, token := range strings.Split(connection, ",") {
			header.Del(strings.TrimSpace(token))
		}
	}
	for _, h := range hopByHopHeaders {
		header.Del(h)
	}
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func makeBackendRequest(r *http.Request, backendURL string) (*http.Request, error) {
	var body io.Reader
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		body = r.Body
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, backendURL, body)
	if err != nil {
		return nil, err
	}

	req.Header = r.Header.Clone()
	removeHopByHopHeaders(req.Header)

	req.Header.Set("X-Forwarded-Host", r.Host)
	if r.TLS != nil {
		req.Header.Set("X-Forwarded-Proto", "https")
	} else {
		req.Header.Set("X-Forwarded-Proto", "http")
	}

	ip := clientIP(r)
	if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
		req.Header.Set("X-Forwarded-For", prior+", "+ip)
	} else {
		req.Header.Set("X-Forwarded-For", ip)
	}

	return req, nil
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func writeCachedResponse(w http.ResponseWriter, entry CacheEntry) {
	copyHeaders(w.Header(), entry.Headers)
	w.Header().Set("X-Cache", "HIT")
	w.WriteHeader(entry.StatusCode)
	if _, err := w.Write(entry.Body); err != nil {
		log.Println("failed writing cached response:", err)
	}
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	recorder := &statusRecorder{ResponseWriter: w}
	start := time.Now()

	metrics.BeginRequest()
	defer func() {
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		metrics.EndRequest(status, time.Since(start))
	}()

	if os.Getenv("DISABLE_RATE_LIMIT") != "1" {
		bucket := clientRateLimiter.get(clientIP(r))
		if !bucket.Allow() {
			metrics.RateLimited.Add(1)
			http.Error(recorder, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	pool, ok := getPoolForRequest(r)
	if !ok {
		http.Error(recorder, "no route configured for this path", http.StatusNotFound)
		return
	}
	pool.Requests.Add(1)

	cacheable := os.Getenv("DISABLE_CACHE") != "1" && isCacheableRequest(r)
	cacheKey := ""

	if cacheable {
		cacheKey = getCacheKey(r)
		if entry, found := responseCache.Get(cacheKey); found {
			metrics.CacheHits.Add(1)
			writeCachedResponse(recorder, entry)
			return
		}
		metrics.CacheMisses.Add(1)
	}

	maxAttempts := 1
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		maxAttempts = 2
	}

	excluded := make(map[int]bool, maxAttempts)
	attemptedBackend := false
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		backendIndex, backend, found := pool.getLeastConnectionsBackend(excluded)
		if !found {
			break
		}

		attemptedBackend = true
		backend.Requests.Add(1)

		backendURL := backend.URL + r.URL.RequestURI()
		proxyReq, err := makeBackendRequest(r, backendURL)
		if err != nil {
			pool.releaseBackend(backendIndex)
			http.Error(recorder, "failed to create backend request", http.StatusInternalServerError)
			return
		}

		resp, err := proxyClient.Do(proxyReq)
		if err != nil {
			lastErr = err
			pool.releaseBackend(backendIndex)
			pool.recordBackendFailure(backendIndex)
			metrics.BackendFailures.Add(1)
			excluded[backendIndex] = true

			if attempt < maxAttempts {
				metrics.Retries.Add(1)
			}
			continue
		}

		safeHeaders := resp.Header.Clone()
		removeHopByHopHeaders(safeHeaders)
		copyHeaders(recorder.Header(), safeHeaders)

		shouldCache := cacheable && resp.StatusCode == http.StatusOK

		if shouldCache {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(maxCacheBodySize+1)))
			if readErr != nil {
				resp.Body.Close()
				pool.releaseBackend(backendIndex)
				pool.recordBackendFailure(backendIndex)
				metrics.BackendFailures.Add(1)
				http.Error(recorder, "failed reading backend response", http.StatusBadGateway)
				return
			}

			if len(body) <= maxCacheBodySize {
				responseCache.Set(cacheKey, CacheEntry{
					StatusCode: resp.StatusCode,
					Headers:    safeHeaders.Clone(),
					Body:       append([]byte(nil), body...),
					ExpiresAt:  time.Now().Add(cacheTTL),
				})

				recorder.Header().Set("X-Cache", "MISS")
				recorder.WriteHeader(resp.StatusCode)
				_, writeErr := recorder.Write(body)

				resp.Body.Close()
				pool.releaseBackend(backendIndex)
				pool.recordBackendSuccess(backendIndex)

				if writeErr != nil {
					log.Println("failed writing backend response:", writeErr)
				}
				return
			}

			recorder.Header().Set("X-Cache", "BYPASS")
			recorder.WriteHeader(resp.StatusCode)

			if _, err := recorder.Write(body); err != nil {
				resp.Body.Close()
				pool.releaseBackend(backendIndex)
				pool.recordBackendFailure(backendIndex)
				metrics.BackendFailures.Add(1)
				return
			}

			_, copyErr := io.Copy(recorder, resp.Body)
			resp.Body.Close()
			pool.releaseBackend(backendIndex)

			if copyErr != nil {
				pool.recordBackendFailure(backendIndex)
				metrics.BackendFailures.Add(1)
				log.Println("failed copying backend response:", copyErr)
			} else {
				pool.recordBackendSuccess(backendIndex)
			}
			return
		}

		recorder.WriteHeader(resp.StatusCode)
		_, copyErr := io.Copy(recorder, resp.Body)
		resp.Body.Close()
		pool.releaseBackend(backendIndex)

		if copyErr != nil {
			pool.recordBackendFailure(backendIndex)
			metrics.BackendFailures.Add(1)
			log.Println("failed copying backend response:", copyErr)
		} else {
			pool.recordBackendSuccess(backendIndex)
		}
		return
	}

	if !attemptedBackend {
		http.Error(recorder, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}

	if isTimeoutError(lastErr) {
		http.Error(recorder, "backend timed out", http.StatusGatewayTimeout)
		return
	}

	http.Error(recorder, "all backend attempts failed", http.StatusBadGateway)
}

func startPprofServer(ctx context.Context) *http.Server {
	if os.Getenv("DISABLE_PPROF") == "1" {
		return nil
	}

	server := &http.Server{
		Addr:              "127.0.0.1:6060",
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		log.Println("pprof running on http://127.0.0.1:6060/debug/pprof/")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("pprof server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	return server
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go startHealthChecker(ctx)
	go clientRateLimiter.cleanup(ctx)
	startPprofServer(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/", proxyHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Println("load balancer running on http://127.0.0.1:8080")
		serverErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown timed out: %v", err)
	}

	proxyTransport.CloseIdleConnections()
	log.Println("shutdown complete")
}
