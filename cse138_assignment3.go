package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

const (
	NumTokens = 5
	MaxHash   = 1024
)

type token struct {
	endpoint string
	value    uint16
}

type view struct {
	nodes  []string
	tokens []token
}

func (v *view) initTokens() {

}

func main() {
	r := mux.NewRouter()

	var view string
	var address string
	view, _ = os.LookupEnv("VIEW")
	address, _ = os.LookupEnv("ADDRESS")
	nodes := strings.Split(view, ",")

	//if addresss matches first ip_addr in view
	if address == nodes[0] {
		// myView = view{nodes: nodes}
		// myView.initTokens()

	} else {

	}

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)
}
