package main

import (
	"fmt"
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

func setupHandler(w http.ResponseWriter, r *http.Request) {

}

func main() {
	r := mux.NewRouter()

	var view string
	var address string
	view, _ = os.LookupEnv("VIEW")
	address, _ = os.LookupEnv("ADDRESS")
	nodes := strings.Split(view, ",")

	//setup a custom request, add a timer and after timer is up send request to the curr node

	//if addresss matches first ip_addr in view
	if address == nodes[0] {
		//calls and gets the view containing nodes array and tokens array
		// myView := view{nodes: nodes}
		// myView.initTokens()

		//listens for incoming requests from the other nodes and responds with the new view
		r.HandleFunc("/kvs/setup", setupHandler).Methods("GET")

	} else {
		//create a post request to the first node to ask for the updated view
		setupReq, err := http.NewRequest("GET", "http://"+nodes[0]+"/kvs/setup", nil)
		if err != nil {
			fmt.Println("Error with creating new request")
		}

		resp, err := http.DefaultClient.Do(setupReq)
		if err != nil {
			fmt.Println("Error when send request to node 1")
		}

		//after sending request and getting response updat the current node's view
		//then start listening to requests as normal

	}

	//start listening for requests whats wrong with having each of the nodes listen immediately?
	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
