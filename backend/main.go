package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run ./backend <port>")
	}

	port := os.Args[1]

	http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("-------- BACKEND", port, "RECIEVED -------")
		fmt.Println("Method:", r.Method)
		fmt.Println("Path:", r.URL.Path)
		fmt.Println("Query:", r.URL.RawQuery)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}

		fmt.Println("Body:", string(body))

		w.Header().Set("X-Backend", port)

		fmt.Fprintf(w, "Hello from backend port %s!\n", port)
	})

	address := ":" + port

	fmt.Println("Backend running on http://localhost:", port)

	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatal(err)
	}
}
