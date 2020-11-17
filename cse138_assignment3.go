package main

import (
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()

	// var VIEW string
	// var exists bool
	VIEW, exists = os.LookupEnv("VIEW")

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)
}
