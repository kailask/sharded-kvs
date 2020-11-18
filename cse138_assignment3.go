package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

//Global constants for system
const (
	NumTokens = 3
	MaxHash   = 1024
)

// global variables
var myView view

type setupRes struct {
	UpdatedView view
	//changes
}

type token struct {
	Endpoint string
	Value    uint32
}

type view struct {
	Nodes  []string
	Tokens []token
}

type change struct {
	Removed bool
	Tokens  []uint32
}

//Register a change for a given token to a change map
func addChange(changes map[string]*change, t *token) {
	if c, exists := changes[t.Endpoint]; exists {
		c.Tokens = append(c.Tokens, t.Value)
	} else {
		changes[t.Endpoint] = &change{Tokens: []uint32{t.Value}}
	}
}

//Generate list of random tokens given map of nodes to add
func generateTokens(addedNodes map[string]bool) []token {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := make([]token, len(addedNodes)*NumTokens)

	for node := range addedNodes {
		for i := 0; i < NumTokens; i++ {
			tokens = append(tokens, token{Endpoint: node, Value: r.Uint32() % MaxHash})
		}
	}

	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].Value < tokens[j].Value
	})

	return tokens
}

//Change view of view struct given new state of active nodes. Returns map of changes
func (v *view) changeView(nodes []string) map[string]*change {
	addedNodes, removedNodes := v.calcNodeDiff(nodes)
	addedTokens := generateTokens(addedNodes)
	tokens, changes, err := v.mergeTokens(addedTokens, addedNodes, removedNodes)

	//Regerate tokens if there were collisions
	for err {
		addedTokens = generateTokens(addedNodes)
		tokens, changes, err = v.mergeTokens(addedTokens, addedNodes, removedNodes)
	}

	v.Nodes = nodes
	v.Tokens = tokens
	return changes
}

//Calculate the added and removed nodes as differences between the view and a given node list
func (v *view) calcNodeDiff(nodes []string) (map[string]bool, map[string]bool) {
	addedNodes, removedNodes := make(map[string]bool), make(map[string]bool)
	nodesMap := make(map[string]int, len(nodes))

	for _, node := range v.Nodes {
		nodesMap[node] += 2
	}

	for _, node := range nodes {
		nodesMap[node] += 3
	}

	for node, val := range nodesMap {
		if val == 2 {
			removedNodes[node] = true
		} else if val == 3 {
			addedNodes[node] = true
		}
	}

	return addedNodes, removedNodes
}

func (v *view) mergeTokens(addedTokens []token, addedNodes map[string]bool, removedNodes map[string]bool) ([]token, map[string]*change, bool) {
	newLength := len(v.Tokens) + len(addedTokens) - (len(removedNodes) * NumTokens)
	changes := make(map[string]*change)
	tokens := make([]token, newLength)

	lastWasChanged := false
	vIndex, aIndex := 0, 0
	var vToken, aToken *token

	if v.Tokens != nil && len(v.Tokens) > 0 {
		vToken = &v.Tokens[vIndex]
	}

	if len(addedTokens) > 0 {
		aToken = &addedTokens[aIndex]
	}

	//Iterate through new tokens list each time adding smallest token from added tokens or view tokens
	for i := 0; aToken != nil || vToken != nil; i++ {
		if vToken != nil {
			//Iterate through view tokens until a token is found that isn't being removed
			for removedNodes[vToken.Endpoint] {
				//Register change for removed token
				if _, exists := changes[vToken.Endpoint]; !exists {
					changes[vToken.Endpoint] = &change{Removed: true}
				}

				vIndex++
				if vIndex < len(v.Tokens) {
					vToken = &v.Tokens[vIndex]
				} else {
					vToken = nil
					break
				}
			}
		}

		//Add smallest token to new list at i from added tokens or view tokens
		if aToken != nil && (vToken == nil || aToken.Value <= vToken.Value) {
			//Detect collisions either between current added and view tokens or current and prev added tokens
			if (vToken != nil && aToken.Value == vToken.Value) || (aIndex > 0 && aToken.Value == addedTokens[aIndex-1].Value) {
				return nil, nil, true
			}

			//Register change for added token
			addChange(changes, aToken)

			//Register change for view token changed by added token
			if i == 0 {
				lastWasChanged = true
			} else if !addedNodes[tokens[i-1].Endpoint] {
				addChange(changes, &tokens[i-1])
			}

			tokens[i] = *aToken
			aIndex++
			if aIndex < len(addedTokens) {
				aToken = &addedTokens[aIndex]
			} else {
				aToken = nil
			}
		} else if vToken != nil {
			//If we are adding the last token to the new list we must notify it if lastWasChanged
			if lastWasChanged && i == len(tokens)-1 {
				addChange(changes, vToken)
			}

			tokens[i] = *vToken
			vIndex++
			if vIndex < len(v.Tokens) {
				vToken = &v.Tokens[vIndex]
			} else {
				vToken = nil
			}
		}
	}

	return tokens, changes, false
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

	http.Handle("/", r)
	http.ListenAndServe(":13800", nil)

}
