package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"server/kvs"
	"strings"

	"github.com/gorilla/mux"
)

//Global constants for system
const (
	NumTokens = 3
	MaxHash   = 1024
)

//Global node state
var (
	MyView = &kvs.View{} //Node's current view
	Active = false       //Is node currently active?
	Setup  setupState    //Used if node is coordinating setup
)

//Used only during setup by first node
type setupState struct {
	initialChanges map[string]*change
}

//Contains data for propogating a view change to another node
type viewChange struct {
	View    view `json:"view"`
	Changes change
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	if !Active {
		remoteAddress := strings.Split(r.RemoteAddr, ":")[0]
		isInView := false

		for _, endpoint := range MyView.Nodes {
			if endpoint == remoteAddress {
				isInView = true
				break
			}
		}

		if isInView {
			viewToSend := viewChange{View: *MyView, Changes: *Setup.initialChanges[remoteAddress]}
			bytes, _ := json.Marshal(viewToSend)
			w.WriteHeader(200)
			w.Write(bytes)
		}

	}

	w.WriteHeader(403)
}

// type LoggingMiddleware func(http.Handler) http.Handler

// func loggingMiddleware(next http.Handler) http.Handler {
//     return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//         // Do stuff here
//         log.Println(r.RequestURI)
//         // Call the next handler, which can be another middleware in the chain, or the final handler.
//         next.ServeHTTP(w, r)
//     })
// }

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

	// r := mux.NewRouter()
	//handlers
	r.HandleFunc("/kvs/init", setupHandler).Methods("GET")
	// r.HandleFunc("/kvs/updateView", updateViewHandler.Mathods("PUT"))
	// r.HandleFunc("/kvs/hello", testHandler)

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
