package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

// global variables
var myView view

//need a map of maps variable as well

//comment
const (
	NumTokens = 5
	MaxHash   = 1024
)

type token struct {
	Endpoint string
	Value    uint32
}

type view struct {
	Nodes  []string
	Tokens []token
}

func (v *view) initTokens() {

}

type setupRes struct {
	UpdatedView view
	//changes
}

func setupHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("in setup handler")
	ipAddr := strings.Split(r.RemoteAddr, ":")[0]
	exists := false
	fmt.Println("ip address of requester is", ipAddr)
	// fmt.Println("nodes is", myView.nodes)

	for _, node := range myView.Nodes {
		if address := strings.Split(node, ":")[0]; address == ipAddr {
			exists = true
			break
		}
	}

	fmt.Println(exists)
	if !exists {
		w.WriteHeader(400)
	} else {
		// send view as is and changes
		fmt.Println("You are in my view!")
		var res setupRes
		res = setupRes{}
		res.UpdatedView = myView
		// fmt.Printf("%+v\n", res)
		bytes, _ := json.Marshal(res)
		w.WriteHeader(200)
		w.Write(bytes)
	}

}

// func testHandler(w http.ResponseWriter, r *http.Request) {
// 	fmt.Fprintf(w, "Hello, world!")
// }

func tests() view {
	fmt.Println("in tests function")
	nodes := []string{"10.10.0.4:13800", "10.10.0.5:13800"}
	vnodes := []token{}
	for i := 0; i < 10; i++ {
		// fmt.Println(i, nodes[i%2])
		t := token{}
		ip := nodes[i%2]
		t.Endpoint = ip
		t.Value = rand.Uint32()
		vnodes = append(vnodes, t)
	}
	v := view{}
	v.Nodes = nodes
	v.Tokens = vnodes
	return v
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
		fmt.Println("coordinator is " + address)
		// myView := view{nodes: nodes}
		// myView.initTokens()
		myView = tests()
		// fmt.Printf("%+v\n", myView)
		fmt.Println("view generated")
	} else if exists {
		//create a get request to the first node to ask for the updated view
		fmt.Println("other node in view is " + address)
		// fmt.Println("url is", "http://"+nodes[0]+"/kvs/setup")
		setupReq, err := http.NewRequest("GET", "http://"+nodes[0]+"/kvs/setup", nil)
		if err != nil {
			fmt.Println("Error with creating new request")
		}

		resp, err := http.DefaultClient.Do(setupReq)
		// fmt.Println("response is", resp)
		if err != nil {
			fmt.Println("Error when sending request to coordinator node")
		} else {
			if resp.StatusCode == 200 {
				bytes, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					fmt.Println("There was an error")
				}
				//unmarshall
				var res setupRes
				res = setupRes{}
				json.Unmarshal(bytes, &res)
				fmt.Printf("responded struct is %+v\n", res)
				myView = res.UpdatedView
			} else {
				fmt.Println("I'm not in your view :(")
			}
		}
	}

	//handlers
	r.HandleFunc("/kvs/setup", setupHandler).Methods("GET")
	// r.HandleFunc("/kvs/updateView", updateViewHandler.Mathods("PUT"))
	// r.HandleFunc("/kvs/hello", testHandler)

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
