package kvs

import (
	"crypto/md5"
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
	NumTokens = 3
	MaxHash   = 1024
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

//ChangeView changes view struct given new state of active nodes. Returns map of changes
func (v *View) ChangeView(nodes []string) map[string]*Change {
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

func binarySearch(Tokens []Token, target uint64) int {
	/*possible index values
	1) index can be an exact match meaning node still exists but takes on a diff range (this node will come from tokens)
		need to find the node next to target node this will give me a new range (target node, next node)
		if a key once recomputed is outside this range, we perform a linear scan to see if node next in line > than key's hash
	2) index not an exact match meaning the node has been removed and thus we find the new node that takes on that key
		this index will be the index of where the node would have been (target node, next node)
		if a key once recomputed is outside this range, we perform a linear scan to see if the node next in line > than key's hash
	*/

	//TODO: also need to handle case when the target node is at the end or beginning
	/* What happens when the index is at the beginning or end? How do we account for this test case?
	1) When the returned index is the first value in the array
		case 1) this index was an exact match, meaning nothing major happens we simply return like normal
		case 2) this is one of the deleted nodes, meaning the target node needs to be set as the last node in tokens
	2) When the returned index is the last value in the array
		case 1) this index was an exact match, meaning its end interval is the first token
		case 2) this was one of the deleted nodes, meaning the target node is one before


	*/

	index := sort.Search(len(Tokens), func(i int) bool { return Tokens[i].Value >= target })
	// interval := []uint64{}
	var endIndex int

	if index < len(Tokens) && Tokens[index].Value == target {
		if index < len(Tokens)-1 {
			// interval = append(interval, target, Tokens[index+1].Value)
			endIndex = index + 1
		} else {
			// interval = append(interval, target, Tokens[0].Value)
			endIndex = 0
		}
	} else {
		if index > 0 && index < len(Tokens) {
			// interval = append(interval, Tokens[index-1].Value, Tokens[index].Value)
			endIndex = index
		} else {
			// interval = append(interval, Tokens[len(Tokens)-1].Value, Tokens[0].Value)
			endIndex = 0
		}
	}
	return endIndex

}

//genereate the position of a key in the hash space
func generateHash(key string) uint64 {
	data := []byte(key)
	// fmt.Println(data)

	num := md5.Sum(data)
	// fmt.Println(num)
	slice := num[8:]
	// fmt.Println(slice)
	bigInt := new(big.Int)
	bigInt.SetBytes(slice)
	decimal := bigInt.Uint64()

	return decimal % MaxHash
}

//perform a linear scan to see what the new shard is
//TDOO: broken you gotta fix this
func linearSearch(Tokens []Token, keyPosition uint64, endIndex int) Token {
	/*
		think about the different cases
		case 1: key is already within interval, meaning key < Tokens[endIndex].Value
		case 2: key outside interval, meaning key > Tokens[endIndex].value
			subcase 1: if the endIndex was the last index in the tokens array, then the node is the first node in the array
			subcase 2: else move forward and check again
	*/
	for keyPosition > Tokens[endIndex].Value {
		//the key is after the last node
		if endIndex == len(Tokens)-1 {
			return Tokens[endIndex]
		}
		endIndex++
	}

	if endIndex == 0 || endIndex == len(Tokens)-1 {
		return Tokens[len(Tokens)-1]
	}
	return Tokens[endIndex-1]

}

func addKeyValue(key string, value string, res map[string]map[string]map[string]string, goalNode Token) {
	//first check if goalNode's endpoint in res
	_, exists := res[goalNode.Endpoint]

	if exists {
		_, ex := res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)]
		if ex {
			res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)][key] = value
		} else {
			kvs := make(map[string]string)
			res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)] = kvs
			res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)][key] = value
		}
	} else {
		kvs := make(map[string]string)
		node := make(map[string]map[string]string)
		res[goalNode.Endpoint] = node
		res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)] = kvs
		res[goalNode.Endpoint][strconv.FormatUint(goalNode.Value, 10)][key] = value
	}

	// if exists {
	// 	gNode := strconv.FormatUint(goalNode.Value, 10)
	// 	res[goalNode.Endpoint][gNode][key] = value
	// } else {
	// 	resvNode := make(map[string]map[string]string)
	// 	reskvs := make(map[string]string)
	// 	reskvs[key] = value
	// 	gNode := strconv.FormatUint(goalNode.Value, 10)
	// 	resvNode[gNode] = reskvs
	// }
}

//Reshard key value pairs
//TODO: change all the uint64 to strings
func (v *View) Reshard(change Change) map[string]map[string]map[string]string {
	removal := change.Removed //check if node removed
	tokens := change.Tokens   //get the node's tokens that are changed
	res := make(map[string]map[string]map[string]string)

	/*possible nodes being repartitioned
	1) node is being removed thus ALL its keys and values are recomputed, we perform binary search per vNode to see the desired destination
	2) node already existing has been updated, thus recompute only some of the keys and values are recomputed, we perfrom binary search per affected vNode
	3) node has just been added, we need to initialize our map of vnodes to key values and we just listen no repartitioning done here
	*/

	if removal { //case 1: node is removed
		for vNode, storage := range KVS {
			endIndex := binarySearch(v.Tokens, vNode)
			for key, value := range storage {
				position := generateHash(key)
				goalToken := linearSearch(v.Tokens, position, endIndex)
				addKeyValue(key, value, res, goalToken)
			}
		}
	} else if len(KVS) == 0 { //case 2: node was just added
		for _, token := range change.Tokens {
			KVS[token] = map[string]string{}
		}
	} else { //case 3: existing node needs to repartition
		for _, token := range tokens {
			endIndex := binarySearch(v.Tokens, token)
			for key, value := range KVS[token] {
				position := generateHash(key)
				goalToken := linearSearch(v.Tokens, position, endIndex)
				//if the goal token return is the same we dont update
				if _, exists := KVS[goalToken.Value]; !exists {
					addKeyValue(key, value, res, goalToken)
					delete(KVS[token], key)
				}
			}
		}
	}

	return res
}
