package main

import (
	"encoding/json"
	"fmt"
	"io"
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
	MyView                = &kvs.View{} //Node's current view
	AmActive              = false       //Is node currently active?
	Setup     *setupState = nil         //Used if node is coordinating setup
	MyAddress string
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

//Used to start setup if current node is first in list
func coordinateSetup(nodes []string) {
	//Remove port numbers
	for i, node := range nodes {
		nodes[i] = strings.Split(node, ":")[0]
	}

	//Initialize local view and kvs
	initialChanges := MyView.ChangeView(nodes)
	kvs.UpdateKVS(*initialChanges[MyAddress])
	AmActive = true

	joinedNodes := make(map[string]bool)
	Setup = &setupState{initialChanges, joinedNodes}
	Setup.nodeJoined(nodes[0])
}

//Unmarshal an http body into a struct
func unmarshalStruct(body io.ReadCloser, s interface{}) interface{} {
	if body != nil {
		defer body.Close()
	}

	bytes, err := ioutil.ReadAll(body)
	if err != nil {
		log.Fatalln(err)
	}

	err = json.Unmarshal(bytes, s)
	if err != nil {
		log.Fatalln(err)
	}
	return s
}

//Try to join the view with the given leader
func joinView(leader string) {
	uri := fmt.Sprintf("http://%s/kvs/int/init", leader)
	res, err := http.Get(uri)
	if err == nil && res.StatusCode == http.StatusOK {
		v := unmarshalStruct(res.Body, &viewChange{}).(*viewChange)
		*MyView = v.View
		kvs.UpdateKVS(v.Changes)
		AmActive = true

		log.Println("Joined view")
	} else {
		log.Println("Unable to join view")
	}
}

//Handle internal setup request to join view
func initHandler(w http.ResponseWriter, r *http.Request) {
	if AmActive && Setup != nil {
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

//Handle internal reshard post request
func internalViewChangeHandler(w http.ResponseWriter, r *http.Request) {
	newKeys := unmarshalStruct(r.Body, map[string]string{}).(map[string]string)
	//handle new keys
}

//Handle internal view change propagation post request
func internalViewChangeHandler(w http.ResponseWriter, r *http.Request) {
	v := unmarshalStruct(r.Body, &viewChange{}).(*viewChange)
	*MyView = v.View
	reshards := kvs.UpdateKVS(v.Changes)
	AmActive = true

	for reshard := range reshards {
		//go routine
	}
}

//Handle external view change put request
func viewChangeHandler(w http.ResponseWriter, r *http.Request) {
	req := unmarshalStruct(r.Body, struct {
		View string `json:"view"`
	}{}).(struct{ View string })

	nodes := strings.Split(req.View, ",")
	oldNodes := MyView.Nodes
	changes := MyView.ChangeView(nodes)

	for node := range oldNodes {
		//go routine
	}
}

func main() {
	r := mux.NewRouter()
	r.Use(loggingMiddleware)

	viewArray, exists := os.LookupEnv("VIEW")
	endpoint, _ := os.LookupEnv("ADDRESS")
	MyAddress = strings.Split(endpoint, ":")[0]
	nodes := strings.Split(viewArray, ",")

	log.Printf("Node starting at %s with view %v\n", endpoint, nodes)

	//if address matches first ip_addr in view
	if endpoint == nodes[0] {
		log.Println("Node coordinating setup")
		coordinateSetup(nodes)
	} else if exists {
		joinView(nodes[0])
	}

	//Internal endpoints
	r.HandleFunc("/kvs/int/init", initHandler).Methods(http.MethodGet)
	r.HandleFunc("/kvs/int/view-change", initHandler).Methods(http.MethodPost)
	r.HandleFunc("/kvs/int/reshard", initHandler).Methods(http.MethodPost)

	//External
	r.HandleFunc("/kvs/view-change", viewChangeHandler).Methods(http.MethodPut)
	// r.HandleFunc("/kvs/updateView", updateViewHandler.Mathods("PUT"))
	// r.HandleFunc("/kvs/hello", testHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+Port, nil)

}
