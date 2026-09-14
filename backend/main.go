package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func helloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("----- BACKEND RECEIVED -----")
	fmt.Println("Method:", r.Method)
	fmt.Println("Path:", r.URL.Path)
	fmt.Println("Query:", r.URL.RawQuery)
	fmt.Println("Host:", r.Host)
	fmt.Println("Headers:", r.Header)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	fmt.Println("Body:", string(body))

	w.Header().Set("X-Backend", "backend-8081")
	fmt.Fprintln(w, "Hello from backend!")
}

func main() {
	http.HandleFunc("/hello", helloHandler)

	fmt.Println("Backend running on http://localhost:8081")

	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		log.Fatal(err)
	}
}
