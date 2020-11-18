package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

// global variables
var myView view

//need a map of maps variable as well

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

type setupRes struct {
	updatedView view
}

func setupHandler(w http.ResponseWriter, r *http.Request) {
	ipAddr := r.RemoteAddr
	exists := false

	for _, node := range myView.nodes {
		if node == ipAddr {
			exists = true
			break
		}
	}

	if !exists {
		w.WriteHeader(400)
	} else {
		var res setupRes
		res = setupRes{}
		res.updatedView = myView
		w.WriteHeader(200)
		bytes, _ := json.Marshal(res)
		w.Write(bytes)

	}

}

func main() {
	r := mux.NewRouter()

	var viewArray string
	var address string
	var exists bool
	viewArray, exists = os.LookupEnv("VIEW")
	address, _ = os.LookupEnv("ADDRESS")
	nodes := strings.Split(viewArray, ",")

	//if address matches first ip_addr in view
	if address == nodes[0] {
		//calls and gets the view containing nodes array and tokens array
		myView := view{nodes: nodes}
		myView.initTokens()
	} else if exists {
		//create a get request to the first node to ask for the updated view
		setupReq, err := http.NewRequest("GET", "http://"+nodes[0]+"/kvs/setup", nil)
		if err != nil {
			fmt.Println("Error with creating new request")
		}

		resp, err := http.DefaultClient.Do(setupReq)
		if err != nil {
			fmt.Println("Error when sending request to coordinator node")
		} else {
			if resp.StatusCode == 200 {
				bytes, _ := ioutil.ReadAll(resp.Body)
				//unmarshall
				var res setupRes
				res = setupRes{}
				json.Unmarshal(bytes, &res)
				myView = res.updatedView
			}
		}

		//after sending request and getting response update the current node's view
		//then start listening to requests as normal

	}

	//handlers
	r.HandleFunc("/kvs/setup", setupHandler).Methods("GET")

	//start listening for requests whats wrong with having each of the nodes listen immediately?
	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
