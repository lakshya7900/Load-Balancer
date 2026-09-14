package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Load balancer recieved: ", r.Method, r.URL.Path)

	backendURL := "http://localhost:8081" + r.URL.RequestURI()

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

func main()  {
	http.HandleFunc("/hello", proxyHandler)

	fmt.Println("Load balancer running on http://localhost:8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}