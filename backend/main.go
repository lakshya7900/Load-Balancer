package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	// "time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run ./backend <port>")
	}

	port := os.Args[1]

	http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}

		fmt.Println("Body:", string(body))

		w.Header().Set("X-Backend", port)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// http.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
	// 	delay := 2 * time.Second

	// 	if port == "8081" {
	// 		delay = 8 * time.Second
	// 	}

	// 	fmt.Println("Slow request on backend", port, "delay:", delay)

	// 	time.Sleep(delay)

	// 	fmt.Fprintf(w, "Slow response from backend %s\n", port)
	// })

	address := ":" + port

	fmt.Println("Backend running on http://localhost:", port)

	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatal(err)
	}
}
