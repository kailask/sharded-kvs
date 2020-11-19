package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"server/kvs"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

//Global node state
var (
	MyView             = &kvs.View{} //Node's current view
	Active             = false       //Is node currently active?
	Setup  *setupState = nil         //Used if node is coordinating setup
)

//Used only during setup by first node
type setupState struct {
	initialChanges map[string]*kvs.Change
	joinedNodes    map[string]bool
}

//Registers node as joined during initial setup
func (s *setupState) nodeJoined(node string) {
	s.joinedNodes[node] = true
	if len(s.joinedNodes) == len(MyView.Nodes) {
		Setup = nil
		Active = true
	}
}

//Contains data for propogating a view change to another node
type viewChange struct {
	View    kvs.View   `json:"view"`
	Changes kvs.Change `json:"changes,omitempty"`
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	if !Active && Setup != nil {
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
			bytes, err := json.Marshal(viewToSend)
			if err != nil {
				w.WriteHeader(http.StatusOK)
				w.Write(bytes)

				Setup.nodeJoined(remoteAddress)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
	w.WriteHeader(http.StatusForbidden)
}

func beginSetup(nodes []string) {

}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Call the next handler
		next.ServeHTTP(w, r)

		log.Printf(
			"%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			time.Since(start),
		)
	})
}

func main() {
	r := mux.NewRouter()
	r.Use(loggingMiddleware)

	viewArray, exists := os.LookupEnv("VIEW")
	address, _ := os.LookupEnv("ADDRESS")
	nodes := strings.Split(viewArray, ",")

	//if address matches first ip_addr in view
	if address == nodes[0] {
		beginSetup(nodes)
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
				// bytes, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					fmt.Println("There was an error")
				}
				//unmarshall
				// var res setupRes
				// res = setupRes{}?
				// json.Unmarshal(bytes, &res)
				// fmt.Printf("responded struct is %+v\n", res)
				// myView = res.UpdatedView
			} else {
				fmt.Println("I'm not in your view :(")
			}
		}
	}

	// r := mux.NewRouter()
	//handlers
	r.HandleFunc("/kvs/init", initHandler).Methods("GET")
	// r.HandleFunc("/kvs/updateView", updateViewHandler.Mathods("PUT"))
	// r.HandleFunc("/kvs/hello", testHandler)

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
