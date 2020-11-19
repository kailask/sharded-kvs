package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"server/kvs"
	"strings"

	"github.com/gorilla/mux"
)

//Port number for all nodes in system
const Port = "13800"

//Global node state
var (
	MyView             = &kvs.View{} //Node's current view
	Active             = false       //Is node currently active?
	Setup  *setupState = nil         //Used if node is coordinating setup
)

//Contains data for propogating a view change to another node
type viewChange struct {
	View    kvs.View   `json:"view"`
	Changes kvs.Change `json:"changes,omitempty"`
}

//Used only during setup by first node
type setupState struct {
	initialChanges map[string]*kvs.Change
	joinedNodes    map[string]bool
}

//Registers node as joined during initial setup and ends setup if all nodes are joined
func (s *setupState) nodeJoined(node string) {
	s.joinedNodes[node] = true
	if len(s.joinedNodes) == len(MyView.Nodes) {
		Setup = nil
		log.Println("Setup complete")
	}
}

func coordinateSetup(nodes []string) {
	//Remove port numbers
	for i, node := range nodes {
		nodes[i] = strings.Split(node, ":")[0]
	}

	//Initialize local view and kvs
	initialChanges := MyView.ChangeView(nodes)
	Active = true

	joinedNodes := make(map[string]bool)
	Setup = &setupState{initialChanges, joinedNodes}
	Setup.nodeJoined(nodes[0])
}

//Try to join the view with the given leader
func joinView(leader string) {
	uri := fmt.Sprintf("http://%s/kvs/int/init", leader)
	res, err := http.Get(uri)
	if err == nil && res.StatusCode == http.StatusOK {
		if err != nil {
			log.Fatalln(err)
		}

		if res.Body != nil {
			defer res.Body.Close()
		}

		bytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatalln(err)
		}

		v := viewChange{}
		err = json.Unmarshal(bytes, &v)
		if err != nil {
			log.Fatalln(err)
		}

		*MyView = v.View
		//TODO: update kvs
		Active = true
		log.Println("Joined view")
	} else {
		log.Println("Unable to join view")
	}
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	if Active && Setup != nil {
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
			if err == nil {
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

func main() {
	r := mux.NewRouter()
	r.Use(loggingMiddleware)

	viewArray, exists := os.LookupEnv("VIEW")
	address, _ := os.LookupEnv("ADDRESS")
	nodes := strings.Split(viewArray, ",")

	log.Printf("Node starting at %s with view %v\n", address, nodes)

	//if address matches first ip_addr in view
	if address == nodes[0] {
		log.Println("Node coordinating setup")
		coordinateSetup(nodes)
	} else if exists {
		joinView(nodes[0])
	}

	//Internal handlers
	r.HandleFunc("/kvs/int/init", initHandler).Methods("GET")
	// r.HandleFunc("/kvs/updateView", updateViewHandler.Mathods("PUT"))
	// r.HandleFunc("/kvs/hello", testHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+Port, nil)

}
