package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	// "time"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run ./backend <port> <service>")
	}

	port := os.Args[1]
	service := os.Args[2]

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Service=%s | Backend=%s | Recieved=%s %s\n", service, port, r.Method, r.URL.Path)
		fmt.Fprintf(w, "Service=%s | Backend=%s | Path=%s\n", service, port, r.URL.Path)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	address := ":" + port

	fmt.Println("Backend running on http://localhost:", port)

	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatal(err)
	}
}
