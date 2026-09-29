package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run ./backend <port> <service>")
	}

	port := os.Args[1]
	service := os.Args[2]

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Dedicated minimal endpoint for raw proxy benchmarks. It intentionally
		// avoids timestamp formatting/logging so the benchmark measures the
		// load-balancer path rather than application work.
		if strings.HasSuffix(r.URL.Path, "/benchmark") {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok\n")
			return
		}

		if strings.HasSuffix(r.URL.Path, "/slow") {
			delay := 2 * time.Second
			if ms := r.URL.Query().Get("ms"); ms != "" {
				if value, err := strconv.Atoi(ms); err == nil && value >= 0 && value <= 10000 {
					delay = time.Duration(value) * time.Millisecond
				}
			}
			time.Sleep(delay)
		}

		if strings.HasSuffix(r.URL.Path, "/hang") {
			time.Sleep(30 * time.Second)
		}

		if strings.HasSuffix(r.URL.Path, "/fail") {
			http.Error(w, "simulated backend failure", http.StatusInternalServerError)
			return
		}

		if strings.HasSuffix(r.URL.Path, "/large") {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			chunk := strings.Repeat("x", 64*1024)
			for i := 0; i < 17; i++ {
				_, _ = io.WriteString(w, chunk)
			}
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(
			w,
			"Service=%s Backend=%s Method=%s Path=%s Time=%s\n",
			service,
			port,
			r.Method,
			r.URL.RequestURI(),
			time.Now().Format(time.RFC3339Nano),
		)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("%s backend running on http://127.0.0.1:%s", service, port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
