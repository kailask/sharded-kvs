package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"server/kvs"
	"strconv"
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

//Contains data for propogating an initial view to a newly added node
type viewInit struct {
	View   kvs.View    `json:"view"`
	Tokens []kvs.Token `json:"tokens"`
}

//Used only during setup by first node
type setupState struct {
	initialChanges map[string][]kvs.Token
	joinedNodes    map[string]bool
}

//Struct containing a value used in get and set handlers
type keyValue struct {
	Value *string `json:"value"`
}

//Registers node as joined during initial setup and ends setup if all nodes are joined
func (s *setupState) nodeJoined(node string) {
	s.joinedNodes[node] = true
	if len(s.joinedNodes) == len(MyView.Nodes) {
		kvs.SetTokens(Setup.initialChanges[MyAddress])
		AmActive = true
		Setup = nil
		log.Println("Setup complete")
	}
}

//Used to start setup if current node is first in list
func coordinateSetup(nodes []string, repl int) {
	//Remove port numbers
	for i, node := range nodes {
		nodes[i] = strings.Split(node, ":")[0]
	}

	//Initialize local view
	tokens := MyView.GenerateView(nodes, repl)

	joinedNodes := make(map[string]bool)
	Setup = &setupState{tokens, joinedNodes}
	Setup.nodeJoined(MyAddress)
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
		kvs.SetTokens(v.Tokens)
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
			viewToSend := viewInit{View: *MyView, Tokens: Setup.initialChanges[remoteAddress]}
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

//Makes post request to uri with given data, returns true on success
func makePost(uri string, data interface{}) bool {
	b, err := json.Marshal(data)
	if err == nil {
		res, err := http.Post(uri, "application/json", bytes.NewBuffer(b))
		return err == nil && res.StatusCode == http.StatusOK
	}
	return false
}

// //Execute an internal get request to another node and return the value
// func executeGet(token kvs.Token, key string) (string, error) {
// 	var value string
// 	tokenValue := strconv.FormatUint(token.Value, 10)
// 	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
// 	res, err := http.Get(uri)
// 	if err != nil {
// 		return value, err
// 	}

// 	if res.StatusCode == http.StatusOK {
// 		if res.Body != nil {
// 			defer res.Body.Close()
// 		}

// 		b, err := ioutil.ReadAll(res.Body)
// 		if err != nil {
// 			return value, err
// 		}

// 		v := keyValue{}
// 		err = json.Unmarshal(b, &v)
// 		if err != nil {
// 			return value, err
// 		}
// 		value = *v.Value
// 		return value, nil
// 	}
// 	return value, errors.New("Node returned not-ok status")
// }

// //Execute an internal set request to another node and return if a key was updated
// func executeSet(token kvs.Token, key string, value keyValue) (bool, error) {
// 	tokenValue := strconv.FormatUint(token.Value, 10)
// 	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
// 	b, err := json.Marshal(value)
// 	if err != nil {
// 		return false, err
// 	}

// 	req, err := http.NewRequest(http.MethodPut, uri, bytes.NewBuffer(b))
// 	if err != nil {
// 		return false, err
// 	}

// 	res, err := http.DefaultClient.Do(req)
// 	if err != nil {
// 		return false, err
// 	}

// 	if res.StatusCode == http.StatusOK {
// 		return true, nil
// 	} else if res.StatusCode == http.StatusCreated {
// 		return false, nil
// 	}
// 	return false, errors.New("Node returned bad status")
// }

// //Execute an internal delete request to another node and return if a key was deleted
// func executeDelete(token kvs.Token, key string) error {
// 	tokenValue := strconv.FormatUint(token.Value, 10)
// 	uri := fmt.Sprintf("http://%s:%s/kvs/int/%s/%s", token.Endpoint, Port, tokenValue, key)
// 	req, err := http.NewRequest(http.MethodDelete, uri, nil)
// 	if err != nil {
// 		return err
// 	}

// 	res, err := http.DefaultClient.Do(req)
// 	if err != nil {
// 		return err
// 	}

// 	if res.StatusCode == http.StatusOK {
// 		return nil
// 	}
// 	return errors.New("Node returned bad status")
// }

// //Handle internal get request with token in url
// func internalGetHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	//Key and token are in url
// 	key := mux.Vars(r)["key"]
// 	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

// 	//Check specified token for key
// 	if v, exists := kvs.Get(token, key); exists {
// 		b, err := json.Marshal(keyValue{Value: &v})

// 		if err == nil {
// 			w.WriteHeader(http.StatusOK)
// 			w.Write(b)
// 		} else {
// 			w.WriteHeader(http.StatusInternalServerError)
// 			log.Println(err)
// 		}
// 	} else {
// 		w.WriteHeader(http.StatusNotFound)
// 	}
// }

// //Handle internal get request with token in url
// func internalSetHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	//Key and token are in url
// 	key := mux.Vars(r)["key"]
// 	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

// 	if r.Body != nil {
// 		defer r.Body.Close()
// 	}

// 	b, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 		return
// 	}

// 	value := keyValue{}
// 	err = json.Unmarshal(b, &value)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 		return
// 	}

// 	//Try to set value
// 	updated, err := kvs.Set(token, key, *value.Value)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 		return
// 	}

// 	if updated {
// 		w.WriteHeader(http.StatusOK)
// 	} else {
// 		w.WriteHeader(http.StatusCreated)
// 	}
// }

// //Handle internal delete request with token in url
// func internalDeleteHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	//Key and token are in url
// 	key := mux.Vars(r)["key"]
// 	token, _ := strconv.ParseUint(mux.Vars(r)["token"], 10, 64)

// 	//Check specified token for key
// 	if err := kvs.Delete(token, key); err == nil {
// 		w.WriteHeader(http.StatusOK)
// 	} else {
// 		w.WriteHeader(http.StatusNotFound)
// 	}
// }

// //Handle external get requests for node's key count
// func keyCountHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	b, err := json.Marshal(struct {
// 		Message  string `json:"message"`
// 		KeyCount int    `json:"key-count"`
// 	}{Message: "Key count retrieved successfully", KeyCount: kvs.KeyCount()})

// 	if err == nil {
// 		w.WriteHeader(http.StatusOK)
// 		w.Write(b)
// 	} else {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 	}
// }

// //Handle external get requests for key
// func getHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	key := mux.Vars(r)["key"]
// 	token := MyView.FindToken(key)
// 	var value *string
// 	res := struct {
// 		DoesExist bool   `json:"doesExist"`
// 		Error     string `json:"error,omitempty"`
// 		Message   string `json:"message"`
// 		Value     string `json:"value,omitempty"`
// 		Address   string `json:"address,omitempty"`
// 	}{}

// 	if token.Endpoint == MyAddress {
// 		//Key would be stored locally
// 		if v, exists := kvs.Get(token.Value, key); exists {
// 			value = &v
// 		}
// 	} else {
// 		//Key would exist on other node
// 		res.Address = token.Endpoint + ":" + Port
// 		returnedValue, err := executeGet(token, key)
// 		if err == nil {
// 			value = &returnedValue
// 		}
// 	}

// 	if value != nil {
// 		res.DoesExist = true
// 		res.Message = "Retrieved successfully"
// 		res.Value = *value
// 		w.WriteHeader(http.StatusOK)
// 	} else {
// 		res.DoesExist = false
// 		res.Error = "Key does not exist"
// 		res.Message = "Error in GET"
// 		w.WriteHeader(http.StatusNotFound)
// 	}

// 	b, err := json.Marshal(res)
// 	if err == nil {
// 		w.Write(b)
// 	} else {
// 		log.Println(err)
// 	}
// }

// //Handle external put requests for key
// func setHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Body != nil {
// 		defer r.Body.Close()
// 	}

// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	b, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 		return
// 	}

// 	res := struct {
// 		Replaced bool   `json:"replaced"`
// 		Error    string `json:"error,omitempty"`
// 		Message  string `json:"message"`
// 		Address  string `json:"address,omitempty"`
// 	}{}
// 	key := mux.Vars(r)["key"]
// 	req := keyValue{}
// 	err = json.Unmarshal(b, &req)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		log.Println(err)
// 		return
// 	}

// 	if req.Value == nil {
// 		res.Error = "Value is missing"
// 		res.Message = "Error in PUT"
// 		w.WriteHeader(http.StatusBadRequest)
// 	} else if len(key) > 50 {
// 		res.Error = "Key is too long"
// 		res.Message = "Error in PUT"
// 		w.WriteHeader(http.StatusBadRequest)
// 	} else {
// 		//Find token for key
// 		token := MyView.FindToken(key)
// 		var updated bool
// 		var err error

// 		if token.Endpoint == MyAddress {
// 			//Key should be stored locally
// 			updated, err = kvs.Set(token.Value, key, *req.Value)

// 		} else {
// 			//Key should exist on other node
// 			res.Address = token.Endpoint + ":" + Port
// 			updated, err = executeSet(token, key, req)
// 		}

// 		if err != nil {
// 			w.WriteHeader(http.StatusInternalServerError)
// 			log.Println(err)
// 			return
// 		}

// 		res.Replaced = updated
// 		if updated {
// 			res.Message = "Updated successfully"
// 			w.WriteHeader(http.StatusOK)
// 		} else {
// 			res.Message = "Added successfully"
// 			w.WriteHeader(http.StatusCreated)
// 		}
// 	}

// 	b, err = json.Marshal(res)
// 	if err == nil {
// 		w.Write(b)
// 	} else {
// 		log.Println(err)
// 	}
// }

// //Handle external get requests for key
// func deleteHandler(w http.ResponseWriter, r *http.Request) {
// 	if !AmActive {
// 		w.WriteHeader(http.StatusForbidden)
// 		return
// 	}

// 	key := mux.Vars(r)["key"]
// 	token := MyView.FindToken(key)
// 	res := struct {
// 		DoesExist bool   `json:"doesExist"`
// 		Error     string `json:"error,omitempty"`
// 		Message   string `json:"message"`
// 		Address   string `json:"address,omitempty"`
// 	}{}

// 	var err error
// 	if token.Endpoint == MyAddress {
// 		//Key would be stored locally
// 		err = kvs.Delete(token.Value, key)
// 	} else {
// 		//Key would exist on other node
// 		res.Address = token.Endpoint + ":" + Port
// 		err = executeDelete(token, key)
// 	}

// 	if err == nil {
// 		res.DoesExist = true
// 		res.Message = "Deleted successfully"
// 		w.WriteHeader(http.StatusOK)
// 	} else {
// 		res.DoesExist = false
// 		res.Error = "Key does not exist"
// 		res.Message = "Error in DELETE"
// 		w.WriteHeader(http.StatusNotFound)
// 	}

// 	b, err := json.Marshal(res)
// 	if err == nil {
// 		w.Write(b)
// 	} else {
// 		log.Println(err)
// 	}
// }

//Print state of system
func debugHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("**************************************")
	fmt.Printf("Address: %s:%s Active: %v\n", MyAddress, Port, AmActive)
	fmt.Printf("Nodes: %v\n", MyView.Nodes)
	fmt.Printf("Tokens: %v\n", MyView.Tokens)
	fmt.Printf("Keys: %v\n", kvs.KeyCount())
	fmt.Println("--------------------------------------")
	for key, partition := range kvs.MyKVS {
		fmt.Printf("%v:\t%v\n", key, partition)
	}
	fmt.Println("**************************************")

	if r.Method == http.MethodGet {
		for _, node := range MyView.Nodes {
			if node != MyAddress {
				makePost(fmt.Sprintf("http://%s:%s/kvs/debug", node, Port), struct{}{})
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
	repl, _ := os.LookupEnv("REPL_FACTOR")

	log.Printf("Node starting at %s with view %v\n", endpoint, nodes)

	//if address matches first ip_addr in view
	if endpoint == nodes[0] {
		log.Println("Node coordinating setup")
		repli, _ := strconv.ParseInt(repl, 10, 64)
		coordinateSetup(nodes, int(repli))
	} else if exists {
		joinView(nodes[0])
	}

	//Internal endpoints
	r.HandleFunc("/kvs/int/init", initHandler).Methods(http.MethodGet)
	// r.HandleFunc("/kvs/int/{token}/{key}", internalGetHandler).Methods(http.MethodGet)
	// r.HandleFunc("/kvs/int/{token}/{key}", internalSetHandler).Methods(http.MethodPut)
	// r.HandleFunc("/kvs/int/{token}/{key}", internalDeleteHandler).Methods(http.MethodDelete)

	// //External endpoints
	// r.HandleFunc("/kvs/key-count", keyCountHandler).Methods(http.MethodGet)
	// r.HandleFunc("/kvs/keys/{key}", getHandler).Methods(http.MethodGet)
	// r.HandleFunc("/kvs/keys/{key}", setHandler).Methods(http.MethodPut)
	// r.HandleFunc("/kvs/keys/{key}", deleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/kvs/debug", debugHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+Port, nil)

}
