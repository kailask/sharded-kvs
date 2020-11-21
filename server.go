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
	"strconv"
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

//Struct containing a value used in get and set handlers
type keyValue struct {
	Value *string `json:"value"`
}

//Key count struct used in building response to view change
type shardCount struct {
	Address  string `json:"address"`
	KeyCount int    `json:"key-count"`
}

//Registers node as joined during initial setup and ends setup if all nodes are joined
func (s *setupState) nodeJoined(node string) {
	s.joinedNodes[node] = true
	if len(s.joinedNodes) == len(MyView.Nodes)-1 {
		MyView.Reshard(*Setup.initialChanges[MyAddress])
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
		MyView.Reshard(v.Changes)
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
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
	w.WriteHeader(http.StatusForbidden)
}

//Get keys counts from shards needed for view change response
func getKeyCounts() ([]shardCount, error) {
	var wg sync.WaitGroup
	wg.Add(len(MyView.Nodes))
	var mutex = &sync.Mutex{}
	shards := map[string]int{}
	for _, node := range MyView.Nodes {
		go getNodeKeyCount(&wg, mutex, node, shards)
	}
	wg.Wait()

	if len(shards) == len(MyView.Nodes) {
		shardArray := make([]shardCount, 0, len(MyView.Nodes))
		for address, count := range shards {
			shardArray = append(shardArray, shardCount{Address: address + ":" + Port, KeyCount: count})
		}
		return shardArray, nil
	}

	return nil, errors.New("Not all key counts were found")
}

//Get key count for single node after view change
func getNodeKeyCount(wg *sync.WaitGroup, mutex *sync.Mutex, node string, shards map[string]int) {
	defer wg.Done()

	if node == MyAddress {
		mutex.Lock()
		shards[node] = kvs.KeyCount()
		mutex.Unlock()
	} else {
		uri := fmt.Sprintf("http://%s:%s/kvs/key-count", node, Port)
		res, err := http.Get(uri)
		if err == nil && res.StatusCode == http.StatusOK {
			if res.Body != nil {
				defer res.Body.Close()
			}

			b, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return
			}

			k := struct {
				KeyCount int `json:"key-count"`
			}{}
			err = json.Unmarshal(b, &k)
			if err != nil {
				return
			}
			mutex.Lock()
			shards[node] = k.KeyCount
			mutex.Unlock()
		}
	}
}

//Notify all nodes of impending view change
func notifyViewChanges(addedNodes map[string]bool, oldNodes []string, changes map[string]*kvs.Change) error {
	var wg sync.WaitGroup
	nodesAccepted := make(map[string]bool)
	nodesNotified := 0
	var mutex = &sync.Mutex{}

	//Notify new nodes of view change and init view
	wg.Add(len(addedNodes))
	for node := range addedNodes {
		nodesNotified++
		v := viewInit{View: *MyView, Changes: *changes[node]}
		go notifyNewView(&wg, mutex, node, v, nodesAccepted)
		delete(changes, node)
	}

	//Notify existing nodes of view change
	for _, node := range oldNodes {
		if !addedNodes[node] && node != MyAddress {
			nodesNotified++
			wg.Add(1)
			go notifyViewChange(&wg, mutex, node, nodesAccepted)
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
	var mutex = &sync.Mutex{}

	//Propagate changes to existing and removed nodes
	wg.Add(len(changes))
	for node, c := range changes {
		go propagateChange(&wg, mutex, node, *c, changesPropagated)
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
func notifyViewChange(wg *sync.WaitGroup, mutex *sync.Mutex, node string, nodesAccepted map[string]bool) {
	defer wg.Done()

	uri := fmt.Sprintf("http://%s:%s/kvs/int/view-change", node, Port)
	if makePost(uri, *MyView) {
		mutex.Lock()
		nodesAccepted[node] = true
		mutex.Unlock()
	}
}

//Routine to notify new node of its initial view state
func notifyNewView(wg *sync.WaitGroup, mutex *sync.Mutex, node string, v viewInit, nodesAccepted map[string]bool) {
	defer wg.Done()

	uri := fmt.Sprintf("http://%s:%s/kvs/int/view-change", node, Port)
	if makePost(uri, v) {
		mutex.Lock()
		nodesAccepted[node] = true
		mutex.Unlock()
	}
}

//Routine to push reshard to changes to another node
func pushReshard(wg *sync.WaitGroup, mutex *sync.Mutex, node string, shard map[string]map[string]string, successfulReshards map[string]bool) {
	defer wg.Done()

	uri := fmt.Sprintf("http://%s:%s/kvs/int/push", node, Port)
	if makePost(uri, shard) {
		mutex.Lock()
		successfulReshards[node] = true
		mutex.Unlock()
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

		//Push new keys to local KVS
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
func propagateChange(wg *sync.WaitGroup, mutex *sync.Mutex, node string, c kvs.Change, changesPropagated map[string]bool) {
	defer wg.Done()

	if node != MyAddress {
		uri := fmt.Sprintf("http://%s:%s/kvs/int/reshard", node, Port)
		if makePost(uri, c) {
			mutex.Lock()
			changesPropagated[node] = true
			mutex.Unlock()
		}
	} else {
		shards := MyView.Reshard(c)
		err := executeReshards(shards)
		if err == nil {
			mutex.Lock()
			changesPropagated[node] = true
			mutex.Unlock()
		}

		if c.Removed {
			AmActive = false
			log.Println("Left view")
		}
	}
}

//Execute an internal get request to another node and return the value
func executeGet(token kvs.Token, key string) (string, error) {
	var value string
	tokenValue := strconv.FormatUint(token.Value, 10)
	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
	res, err := http.Get(uri)
	if err != nil {
		return value, err
	}

	if res.StatusCode == http.StatusOK {
		if res.Body != nil {
			defer res.Body.Close()
		}

		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return value, err
		}

		v := keyValue{}
		err = json.Unmarshal(b, &v)
		if err != nil {
			return value, err
		}
		value = *v.Value
		return value, nil
	}
	return value, errors.New("Node returned not-ok status")
}

//Execute an internal set request to another node and return if a key was updated
func executeSet(token kvs.Token, key string, value keyValue) (bool, error) {
	tokenValue := strconv.FormatUint(token.Value, 10)
	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
	b, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequest(http.MethodPut, uri, bytes.NewBuffer(b))
	if err != nil {
		return false, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	if res.StatusCode == http.StatusOK {
		return true, nil
	} else if res.StatusCode == http.StatusCreated {
		return false, nil
	}
	return false, errors.New("Node returned bad status")
}

//Execute an internal delete request to another node and return if a key was deleted
func executeDelete(token kvs.Token, key string) error {
	tokenValue := strconv.FormatUint(token.Value, 10)
	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
	req, err := http.NewRequest(http.MethodDelete, uri, nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode == http.StatusOK {
		return nil
	}
	return errors.New("Node returned bad status")
}

//Execute all reshards from this node
func executeReshards(shards map[string]map[string]map[string]string) error {
	var wg sync.WaitGroup
	wg.Add(len(shards))
	var mutex = &sync.Mutex{}
	successfulReshards := make(map[string]bool)

	for node, shard := range shards {
		//Push resharded keys to respective nodes
		go pushReshard(&wg, mutex, node, shard, successfulReshards)
	}

	wg.Wait()

	if len(successfulReshards) == len(shards) {
		return nil
	}
	return errors.New("Not all reshards completed")
}

//Handle internal reshard post request with changes
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

		shards := MyView.Reshard(c)
		err = executeReshards(shards)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		//Become inactive if removed from view
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
		MyView.Reshard(v.Changes)
		AmActive = true
		w.WriteHeader(http.StatusOK)

		log.Println("Joined view")
	}
}

//Handle internal get request with token in url
func internalGetHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	//Key and token are in url
	key := mux.Vars(r)["key"]
	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

	//Check specified token for key
	if v, exists := kvs.Get(token, key); exists {
		b, err := json.Marshal(keyValue{Value: &v})

		if err == nil {
			w.WriteHeader(http.StatusOK)
			w.Write(b)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

//Handle internal get request with token in url
func internalSetHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	//Key and token are in url
	key := mux.Vars(r)["key"]
	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

	if r.Body != nil {
		defer r.Body.Close()
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	value := keyValue{}
	err = json.Unmarshal(b, &value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	//Try to set value
	updated, err := kvs.Set(token, key, *value.Value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	if updated {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

//Handle internal delete request with token in url
func internalDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	//Key and token are in url
	key := mux.Vars(r)["key"]
	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

	//Check specified token for key
	if err := kvs.Delete(token, key); err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
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
		log.Println(err)
		return
	}

	req := struct {
		View string `json:"view"`
	}{}
	err = json.Unmarshal(b, &req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
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

	log.Println("View updated to", nodes)

	//Get keys counts from shards
	shardCounts, err := getKeyCounts()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, err = json.Marshal(struct {
		Message string       `json:"message"`
		Shards  []shardCount `json:"shards"`
	}{Message: "View change successful", Shards: shardCounts})
	if err == nil {
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
	}
}

//Handle external get requests for node's key count
func keyCountHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	b, err := json.Marshal(struct {
		Message  string `json:"message"`
		KeyCount int    `json:"key-count"`
	}{Message: "Key count retrieved successfully", KeyCount: kvs.KeyCount()})

	if err == nil {
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
	}
}

//Handle external get requests for key
func getHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
		//Key would be stored locally
		if v, exists := kvs.Get(token.Value, key); exists {
			value = &v
		}
	} else {
		//Key would exist on other node
		res.Address = token.Endpoint + ":" + Port
		returnedValue, err := executeGet(token, key)
		if err == nil {
			value = &returnedValue
		}
	}

	if value != nil {
		res.DoesExist = true
		res.Message = "Retrieved successfully"
		res.Value = *value
		w.WriteHeader(http.StatusOK)
	} else {
		res.DoesExist = false
		res.Error = "Key does not exist"
		res.Message = "Error in GET"
		w.WriteHeader(http.StatusNotFound)
	}

	b, err := json.Marshal(res)
	if err == nil {
		w.Write(b)
	} else {
		log.Println(err)
	}
}

//Handle external put requests for key
func setHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Println(err)
		return
	}

	res := struct {
		Replaced bool   `json:"replaced"`
		Error    string `json:"error,omitempty"`
		Message  string `json:"message"`
		Address  string `json:"address,omitempty"`
	}{}
	key := mux.Vars(r)["key"]
	req := keyValue{}
	err = json.Unmarshal(b, &req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	if req.Value == nil {
		res.Error = "Value is missing"
		res.Message = "Error in PUT"
		w.WriteHeader(http.StatusBadRequest)
	} else if len(key) > 50 {
		res.Error = "Key is too long"
		res.Message = "Error in PUT"
		w.WriteHeader(http.StatusBadRequest)
	} else {
		//Find token for key
		token := MyView.FindToken(key)
		var updated bool
		var err error

		if token.Endpoint == MyAddress {
			//Key should be stored locally
			updated, err = kvs.Set(token.Value, key, *req.Value)

		} else {
			//Key should exist on other node
			res.Address = token.Endpoint + ":" + Port
			updated, err = executeSet(token, key, req)
		}

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}

		res.Replaced = updated
		if updated {
			res.Message = "Updated successfully"
			w.WriteHeader(http.StatusOK)
		} else {
			res.Message = "Added successfully"
			w.WriteHeader(http.StatusCreated)
		}
	}

	b, err = json.Marshal(res)
	if err == nil {
		w.Write(b)
	} else {
		log.Println(err)
	}
}

//Handle external get requests for key
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if !AmActive {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	key := mux.Vars(r)["key"]
	token := MyView.FindToken(key)
	res := struct {
		DoesExist bool   `json:"doesExist"`
		Error     string `json:"error,omitempty"`
		Message   string `json:"message"`
		Address   string `json:"address,omitempty"`
	}{}

	var err error
	if token.Endpoint == MyAddress {
		//Key would be stored locally
		err = kvs.Delete(token.Value, key)
	} else {
		//Key would exist on other node
		res.Address = token.Endpoint + ":" + Port
		err = executeDelete(token, key)
	}

	if err == nil {
		res.DoesExist = true
		res.Message = "Deleted successfully"
		w.WriteHeader(http.StatusOK)
	} else {
		res.DoesExist = false
		res.Error = "Key does not exist"
		res.Message = "Error in DELETE"
		w.WriteHeader(http.StatusNotFound)
	}

	b, err := json.Marshal(res)
	if err == nil {
		w.Write(b)
	} else {
		log.Println(err)
	}
}

//Print state of system
func debugHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("**************************************")
	fmt.Printf("Address: %s:%s Active: %v\n", MyAddress, Port, AmActive)
	fmt.Printf("Nodes: %v\n", MyView.Nodes)
	fmt.Printf("Tokens: %v\n", MyView.Tokens)
	fmt.Printf("Keys: %v\n", kvs.KeyCount())
	fmt.Println("--------------------------------------")
	for key, partition := range kvs.KVS {
		fmt.Printf("%v:\t%v\n", key, partition)
	}
	fmt.Println("**************************************")

	if r.Method == http.MethodGet {
		for _, node := range MyView.Nodes {
			if node != MyAddress && !makePost(fmt.Sprintf("http://%s:%s/kvs/debug", node, Port), struct{}{}) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
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
	r.HandleFunc("/kvs/int/{token}/{key}", internalGetHandler).Methods(http.MethodGet)
	r.HandleFunc("/kvs/int/{token}/{key}", internalSetHandler).Methods(http.MethodPut)
	r.HandleFunc("/kvs/int/{token}/{key}", internalDeleteHandler).Methods(http.MethodDelete)

	//External endpoints
	r.HandleFunc("/kvs/view-change", viewChangeHandler).Methods(http.MethodPut)
	r.HandleFunc("/kvs/key-count", keyCountHandler).Methods(http.MethodGet)
	r.HandleFunc("/kvs/keys/{key}", getHandler).Methods(http.MethodGet)
	r.HandleFunc("/kvs/keys/{key}", setHandler).Methods(http.MethodPut)
	r.HandleFunc("/kvs/keys/{key}", deleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/kvs/debug", debugHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+Port, nil)

}
