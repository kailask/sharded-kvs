package kvs

import (
	"crypto/md5"
	"errors"
	"math/big"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

//KVS maps token values to k:v maps
var KVS = map[uint64]map[string]string{}

//Global constants for kvs
const (
	NumTokens = 10
	MaxHash   = 8192
)

//Token contains an ip address and value in has space
type Token struct {
	Endpoint string `json:"endpoint"`
	Value    uint64 `json:"value"`
}

//View contains list of current nodes and their sorted tokens
type View struct {
	Nodes  []string `json:"nodes"`
	Tokens []Token  `json:"tokens"`
}

//Change is the changes to a single node during a view change
type Change struct {
	Removed bool     `json:"removed"`
	Tokens  []uint64 `json:"tokens,omitempty"`
}

//Get returns the value given the key and token
func Get(token uint64, key string) (string, bool) {
	value, exists := KVS[token][key]
	return value, exists
}

//Set sets the key and value at the given token. Returns if updated or error
func Set(token uint64, key string, value string) (bool, error) {
	if partition, exists := KVS[token]; exists {
		_, updated := partition[key]
		partition[key] = value
		return updated, nil
	}
	return false, errors.New("Partition does not exist")
}

//Delete deletes the key in the given token
func Delete(token uint64, key string) error {
	_, exists := KVS[token][key]
	if exists {
		delete(KVS[token], key)
		return nil
	}
	return errors.New("Key does not exist")
}

//KeyCount returns the current key count of the KVS
func KeyCount() int {
	keyCount := 0
	for _, token := range KVS {
		keyCount += len(token)
	}
	return keyCount
}

//PushKeys tries to update the KVS with the new keys. Returns error if issue
func PushKeys(newKeys map[string]map[string]string) error {
	for name, shard := range newKeys {
		key, _ := strconv.ParseUint(name, 10, 64)
		if partition, exists := KVS[key]; exists {
			for k, v := range shard {
				partition[k] = v
			}
		} else {
			return errors.New("Partition does not exist")
		}
	}
	return nil
}

//FindToken returns the token corresponding to a given key
func (v *View) FindToken(key string) Token {
	return v.Tokens[v.findTokenIndex(generateHash(key))]
}

//ChangeView changes view struct given new state of active nodes. Returns map of changes and map of new nodes
func (v *View) ChangeView(nodes []string) (map[string]*Change, map[string]bool) {
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
	return changes, addedNodes
}

//Reshard key value pairs
func (v *View) Reshard(change Change) map[string]map[string]map[string]string {
	removal := change.Removed //check if node removed
	tokens := change.Tokens   //get the node's tokens that are changed
	res := make(map[string]map[string]map[string]string)

	if removal { //case 1: node is removed
		for vNode, storage := range KVS {
			for key, value := range storage {
				position := generateHash(key)
				startIndex := v.findTokenIndex(position)
				addKeyValue(key, value, res, v.Tokens[startIndex])
			}
			delete(KVS, vNode)
		}
	} else if len(KVS) == 0 { //case 2: node was just added
		for _, token := range change.Tokens {
			KVS[token] = map[string]string{}
		}
	} else { //case 3: existing node needs to repartition
		for _, token := range tokens {
			for key, value := range KVS[token] {
				position := generateHash(key)
				startIndex := v.findTokenIndex(position)
				if _, exists := KVS[v.Tokens[startIndex].Value]; !exists {
					addKeyValue(key, value, res, v.Tokens[startIndex])
					delete(KVS[token], key)
				}
			}
		}
	}

	return res
}

//Calculate the added and removed nodes as differences between the view and a given node list
func (v *View) calcNodeDiff(nodes []string) (map[string]bool, map[string]bool) {
	addedNodes, removedNodes := make(map[string]bool), make(map[string]bool)
	nodesMap := make(map[string]uint64, len(nodes))

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

func (v *View) mergeTokens(addedTokens []Token, addedNodes map[string]bool, removedNodes map[string]bool) ([]Token, map[string]*Change, bool) {
	newLength := len(v.Tokens) + len(addedTokens) - (len(removedNodes) * NumTokens)
	changes := make(map[string]*Change)
	tokens := make([]Token, newLength)

	lastWasChanged := false
	vIndex, aIndex := 0, 0
	var vToken, aToken *Token

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
					changes[vToken.Endpoint] = &Change{Removed: true}
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

//Register a change for a given token to a change map
func addChange(changes map[string]*Change, t *Token) {
	if c, exists := changes[t.Endpoint]; exists {
		c.Tokens = append(c.Tokens, t.Value)
	} else {
		changes[t.Endpoint] = &Change{Tokens: []uint64{t.Value}}
	}
}

//Generate list of random tokens given map of nodes to add
func generateTokens(addedNodes map[string]bool) []Token {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := []Token{}

	for node := range addedNodes {
		for i := 0; i < NumTokens; i++ {
			tokens = append(tokens, Token{Endpoint: node, Value: r.Uint64() % MaxHash})
		}
	}

	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].Value < tokens[j].Value
	})

	return tokens
}

func (v *View) findTokenIndex(target uint64) int {
	index := sort.Search(len(v.Tokens), func(i int) bool { return v.Tokens[i].Value >= target })
	var startIndex int

	if index < len(v.Tokens) && v.Tokens[index].Value == target {
		startIndex = index
	} else {
		if index == 0 {
			startIndex = len(v.Tokens) - 1
		} else {
			startIndex = index - 1
		}
	}

	return startIndex
}

//genereate the position of a key in the hash space
func generateHash(key string) uint64 {
	hash := md5.Sum([]byte(key))
	bigInt := new(big.Int).SetBytes(hash[8:])
	return bigInt.Uint64() % MaxHash
}

func addKeyValue(key string, value string, res map[string]map[string]map[string]string, goalNode Token) {
	//first check if goalNode's endpoint in res
	_, exists := res[goalNode.Endpoint]
	partition := strconv.FormatUint(goalNode.Value, 10)

	if exists {
		_, ex := res[goalNode.Endpoint][partition]
		if ex {
			res[goalNode.Endpoint][partition][key] = value
		} else {
			kvs := make(map[string]string)
			res[goalNode.Endpoint][partition] = kvs
			res[goalNode.Endpoint][partition][key] = value
		}
	} else {
		kvs := make(map[string]string)
		node := make(map[string]map[string]string)
		res[goalNode.Endpoint] = node
		res[goalNode.Endpoint][partition] = kvs
		res[goalNode.Endpoint][partition][key] = value
	}
}
