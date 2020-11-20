package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"server/kvs"
	"strings"
	"sync"

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

//Contains data for propogating an initial view to a newly added node
type viewInit struct {
	View    kvs.View   `json:"view"`
	Changes kvs.Change `json:"changes"`
}

//Used only during setup by first node
type setupState struct {
	initialChanges map[string]*kvs.Change
	joinedNodes    map[string]bool
}

//Registers node as joined during initial setup and ends setup if all nodes are joined
func (s *setupState) nodeJoined(node string) {
	s.joinedNodes[node] = true
	if len(s.joinedNodes) == len(MyView.Nodes)-1 {
		MyView.UpdateKVS(*Setup.initialChanges[MyAddress])
		AmActive = true
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

	//Initialize local view
	initialChanges, _ := MyView.ChangeView(nodes)

	joinedNodes := make(map[string]bool)
	Setup = &setupState{initialChanges, joinedNodes}
}

//Try to join the view with the given leader
func joinView(leader string) {
	uri := fmt.Sprintf("http://%s/kvs/int/init", leader)
	res, err := http.Get(uri)
	if err == nil && res.StatusCode == http.StatusOK {
		if res.Body != nil {
			defer res.Body.Close()
		}

		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatalln(err)
		}

		v := viewInit{}
		err = json.Unmarshal(b, &v)
		if err != nil {
			log.Fatalln(err)
		}

		*MyView = v.View
		MyView.UpdateKVS(v.Changes)
		AmActive = true

		log.Println("Joined view")
	} else {
		log.Println("Unable to join view")
	}
}

//Handle internal setup request to join view
func initHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive && Setup != nil {
		remoteAddress := strings.Split(r.RemoteAddr, ":")[0]
		isInView := false

		for _, endpoint := range MyView.Nodes {
			if endpoint == remoteAddress {
				isInView = true
				break
			}
		}

		if isInView {
			viewToSend := viewInit{View: *MyView, Changes: *Setup.initialChanges[remoteAddress]}
			b, err := json.Marshal(viewToSend)
			if err == nil {
				w.WriteHeader(http.StatusOK)
				w.Write(b)

				Setup.nodeJoined(remoteAddress)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
	w.WriteHeader(http.StatusForbidden)
}

//Notify all nodes of impending view change
func notifyViewChanges(addedNodes map[string]bool, oldNodes []string, changes map[string]*kvs.Change) error {
	var wg sync.WaitGroup
	nodesAccepted := make(map[string]bool)
	nodesNotified := 0

	//Notify new nodes of view change and init view
	wg.Add(len(addedNodes))
	for node := range addedNodes {
		nodesNotified++
		v := viewInit{View: *MyView, Changes: *changes[node]}
		go notifyNewView(&wg, node, v, nodesAccepted)
		delete(changes, node)
	}

	//Notify existing nodes of view change
	for _, node := range oldNodes {
		if !addedNodes[node] && node != MyAddress {
			nodesNotified++
			wg.Add(1)
			go notifyViewChange(&wg, node, nodesAccepted)
		}
	}
	wg.Wait()

	if len(nodesAccepted) == nodesNotified {
		return nil
	}
	return errors.New("Not all nodes accepted view change")
}

//Propagate changes to all necessary nodes
func propagateViewChanges(changes map[string]*kvs.Change) error {
	var wg sync.WaitGroup
	changesPropagated := make(map[string]bool)

	//Propagate changes to existing and removed nodes
	wg.Add(len(changes))
	for node, c := range changes {
		go propagateChange(&wg, node, *c, changesPropagated)
	}

	wg.Wait()

	if len(changesPropagated) == len(changes) {
		return nil
	}
	return errors.New("Not all nodes propagated changes")
}

//Makes post request to uri with given data, returns true on success
func makePost(uri string, data interface{}) bool {
	b, err := json.Marshal(data)
	if err == nil {
		res, err := http.Post(uri, "application/json", bytes.NewBuffer(b))
		return err == nil && res.StatusCode == http.StatusOK
	}
	return false
}

//Routine to notify existing node of updated view
func notifyViewChange(wg *sync.WaitGroup, node string, nodesAccepted map[string]bool) {
	defer wg.Done()

	uri := fmt.Sprintf("http://%s:%s/kvs/int/view-change", node, Port)
	if makePost(uri, *MyView) {
		nodesAccepted[node] = true
	}
}

//Routine to notify new node of its initial view state
func notifyNewView(wg *sync.WaitGroup, node string, v viewInit, nodesAccepted map[string]bool) {
	defer wg.Done()

	uri := fmt.Sprintf("http://%s:%s/kvs/int/view-change", node, Port)
	if makePost(uri, v) {
		nodesAccepted[node] = true
	}
}

//Handle keys pushed to node during reshard
func pushHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer r.Body.Close()
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if AmActive {
		newKeys := make(map[string]map[string]string)
		err = json.Unmarshal(b, &newKeys)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = kvs.PushKeys(newKeys)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	} else {
		w.WriteHeader(http.StatusForbidden)
	}
}

//Routine to propagate a change to node
func propagateChange(wg *sync.WaitGroup, node string, c kvs.Change, changesPropagated map[string]bool) {
	defer wg.Done()

	if node != MyAddress {
		uri := fmt.Sprintf("http://%s:%s/kvs/int/reshard", node, Port)
		if makePost(uri, c) {
			changesPropagated[node] = true
		}
	} else {
		shards := map[string]map[string]map[string]string{} //TODO: reshard()
		err := executeReshards(shards)
		if err == nil {
			changesPropagated[node] = true
		}

		if c.Removed {
			AmActive = false
			log.Println("Left view")
		}
	}
}

//Execute all reshards from this node
func executeReshards(shards map[string]map[string]map[string]string) error {
	var wg sync.WaitGroup
	wg.Add(len(shards))
	successfulReshards := make(map[string]bool)

	for node, shard := range shards {
		go func(wg *sync.WaitGroup, node string, shard map[string]map[string]string, successfulReshards map[string]bool) {
			defer wg.Done()

			uri := fmt.Sprintf("http://%s:%s/kvs/int/push", node, Port)
			if makePost(uri, shard) {
				successfulReshards[node] = true
			}
		}(&wg, node, shard, successfulReshards)
	}

	wg.Wait()

	if len(successfulReshards) == len(shards) {
		return nil
	}
	return errors.New("Not all reshards completed")
}

//Handle internal reshard post request
func reshardHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer r.Body.Close()
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if AmActive {
		c := kvs.Change{}
		err = json.Unmarshal(b, &c)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		shards := map[string]map[string]map[string]string{} //TODO: reshard()
		err = executeReshards(shards)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		if c.Removed {
			AmActive = false
			log.Println("Left view")
		}
	} else {
		w.WriteHeader(http.StatusForbidden)
	}
}

// Handle internal view change propagation post request
func internalViewChangeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer r.Body.Close()
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if AmActive {
		//I am already part of this view
		v := kvs.View{}
		err = json.Unmarshal(b, &v)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		*MyView = v
		w.WriteHeader(http.StatusOK)
	} else {
		//I am a new node
		v := viewInit{}
		err = json.Unmarshal(b, &v)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		*MyView = v.View
		MyView.UpdateKVS(v.Changes)
		AmActive = true
		w.WriteHeader(http.StatusOK)

		log.Println("Joined view")
	}
}

//Handle external view change put request, node acts as coordinator
func viewChangeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer r.Body.Close()
	}

	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	req := struct {
		View string `json:"view"`
	}{}
	err = json.Unmarshal(b, &req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nodes := strings.Split(req.View, ",")
	for i, node := range nodes {
		nodes[i] = strings.Split(node, ":")[0]
	}

	//Update my view
	oldNodes := MyView.Nodes
	changes, addedNodes := MyView.ChangeView(nodes)

	//Update other's views
	err = notifyViewChanges(addedNodes, oldNodes, changes)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Propagate view changes
	err = propagateViewChanges(changes)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// for node := range nodes {

	// }
	//TODO: get reponse with key counts
	w.WriteHeader(http.StatusOK)

	log.Println("View updated to", nodes)
}

//Handle external get requests for node's key count
func keyCountHandler(w http.ResponseWriter, r *http.Request) {
	if AmActive {
		b, err := json.Marshal(struct {
			Message  string `json:"message"`
			KeyCount int    `json:"key-count"`
		}{Message: "Key count retrieved successfully", KeyCount: kvs.KeyCount()})

		if err == nil {
			w.WriteHeader(http.StatusOK)
			w.Write(b)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}

	} else {
		w.WriteHeader(http.StatusForbidden)
	}
}

//Handle external get requests for key
func getHandler(w http.ResponseWriter, r *http.Request) {
	if AmActive {
		key := mux.Vars(r)["key"]
		token := MyView.FindToken(key)
		var value *string
		res := struct {
			DoesExist bool   `json:"doesExist"`
			Error     string `json:"error,omitempty"`
			Message   string `json:"message"`
			Value     string `json:"value,omitempty"`
			Address   string `json:"address,omitempty"`
		}{}

		if token.Endpoint == MyAddress {
			if v, exists := kvs.KVS[token.Value][key]; exists {
				*value = v
			}
		} else {
			//TODO query endpoint
		}

		if value != nil {
			res.DoesExist = true
			res.Message = "Retrieved successfully"
			res.Value = *value

			b, err := json.Marshal(res)
			if err == nil {
				w.WriteHeader(http.StatusOK)
				w.Write(b)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		} else {
			res.DoesExist = false
			res.Error = "Key does not exist"
			res.Message = "Error in GET"

			b, err := json.Marshal(res)
			if err == nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write(b)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
	} else {
		w.WriteHeader(http.StatusForbidden)
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
	r.HandleFunc("/kvs/int/view-change", internalViewChangeHandler).Methods(http.MethodPost)
	r.HandleFunc("/kvs/int/reshard", reshardHandler).Methods(http.MethodPost)
	r.HandleFunc("/kvs/int/push", pushHandler).Methods(http.MethodPost)
	// r.HandleFunc("/kvs/int/{key}/{token}", internalGetHandler).Methods(http.MethodGet)

	//External endpoints
	r.HandleFunc("/kvs/view-change", viewChangeHandler).Methods(http.MethodPut)
	r.HandleFunc("/kvs/key-count", keyCountHandler).Methods(http.MethodGet)
	r.HandleFunc("/kvs/{key}", getHandler).Methods(http.MethodGet)
	// r.HandleFunc("/kvs/{key}", setHandler).Methods(http.MethodPut)
	// r.HandleFunc("/kvs/{key}", deleteHandler).Methods(http.MethodDelete)

	http.Handle("/", r)
	http.ListenAndServe(":"+Port, nil)

}
