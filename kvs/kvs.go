package kvs

import (
	"container/heap"
	"crypto/md5"
	"errors"
	"math/big"
	"math/rand"
	"sort"
	"time"
)

//KVS is a string:string key value store
type KVS map[string]string

//PartitionedKVS is a string:string kvs divided into a map of partitions
type PartitionedKVS map[uint64]KVS

//MyKVS maps token values to k:v maps
var MyKVS = PartitionedKVS{}

//Global constants for kvs
const (
	NumTokens = 200
	MaxHash   = 1000000
)

//Token contains an ip address and value in has space
type Token struct {
	Shard uint64 `json:"shard"`
	Value uint64 `json:"value"`
}

//View contains list of current nodes and their sorted tokens
type View struct {
<<<<<<< HEAD
	Nodes      []string            `json:"nodes"`
	Tokens     []Token             `json:"tokens"`
	ShardsList map[uint64][]string `json:"shardslist"`
}

//Change is the changes to a single node during a view change
type Change struct {
	Removed bool     `json:"removed"`
	Tokens  []uint64 `json:"tokens,omitempty"`
=======
	Nodes  []string `json:"nodes"`
	Tokens []Token  `json:"tokens"`
	Shards map[uint64][]string
>>>>>>> 1317bf24c928095dfbdbe33276a1e5c8b6665ad2
}

//Get returns the value given the key and token
func Get(token uint64, key string) (string, bool) {
	value, exists := MyKVS[token][key]
	return value, exists
}

//Set sets the key and value at the given token. Returns if updated or error
func Set(token uint64, key string, value string) (bool, error) {
	if partition, exists := MyKVS[token]; exists {
		_, updated := partition[key]
		partition[key] = value
		return updated, nil
	}
	return false, errors.New("Partition does not exist")
}

//Delete deletes the key in the given token
func Delete(token uint64, key string) error {
	_, exists := MyKVS[token][key]
	if exists {
		delete(MyKVS[token], key)
		return nil
	}
	return errors.New("Key does not exist")
}

//KeyCount returns the current key count of the KVS
func KeyCount() int {
	keyCount := 0
	for _, token := range MyKVS {
		keyCount += len(token)
	}
	return keyCount
}

//FindToken returns the token corresponding to a given key
func (v *View) FindToken(key string) Token {
	hash := generateHash(key)
	index := sort.Search(len(v.Tokens), func(i int) bool { return v.Tokens[i].Value >= hash })
	tokenIndex := index - 1

	if index < len(v.Tokens) && v.Tokens[index].Value == hash {
		tokenIndex = index
	} else if index == 0 {
		tokenIndex = len(v.Tokens) - 1
	}

	return v.Tokens[tokenIndex]
}

//BuildHeap builds a heap
func (v *View) BuildHeap(storage map[string]bool, maxHeap PriorityQueue, r int) []string {
	i := 0
	movedEndpoints := []string{}
	for shard, endpoints := range v.ShardsList {
		count := 1
		sameEndpoints := []string{}
		for _, endpoint := range endpoints {
			if _, ok := storage[endpoint]; ok {
				if count > r {
					movedEndpoints = append(movedEndpoints, endpoint)
				} else {
					sameEndpoints = append(sameEndpoints, endpoint)
					count++
				}
				storage[endpoint] = false
			}
		}
		maxHeap[i] = &Item{
			Nodes:    sameEndpoints,
			Shard:    shard,
			Priority: count,
			Index:    i,
		}
		i++
	}
	heap.Init(&maxHeap)

	return movedEndpoints

}

//PlaceNode into an available shard
func (v *View) PlaceNode(movedNodes []string, shardsList map[uint64][]string, r int) {
	pointer := 0

	for shard := range shardsList {
		for len(shardsList[shard]) < r {
			shardsList[shard] = append(shardsList[shard], movedNodes[pointer])
			pointer++
		}
	}
}

//CreateShardList creates a new shard list given a new view. Returns the new shard list for a view and potentially changes as well.
func (v *View) CreateShardList(nodes []string, r int) (map[uint64][]string, []uint64, []uint64, []uint64) {
	//map to store the new view and allows for constant lookup
	storage := map[string]bool{}
	for _, node := range nodes {
		storage[node] = true
	}

	//get the prev repl factor, wonder if theres a better way to solve this
	var prevReplFactor int
	for _, v := range v.ShardsList {
		prevReplFactor = len(v)
		break
	}

	prevNumShards := len(v.ShardsList)
	newNumShards := len(nodes) / r

	//build heap and get all the nodes that need to be moved around
	maxHeap := make(PriorityQueue, prevNumShards)
	movedNodes := v.BuildHeap(storage, maxHeap, r)

	//pop off heap and construct new shards list
	// Take the items out; they arrive in decreasing priority order. Take out items and keep track of how many shards are still in the heap
	res := make(map[uint64][]string)
	modifiedShards := []uint64{}
	count := 0
	for maxHeap.Len() > 0 && count < newNumShards {
		item := heap.Pop(&maxHeap).(*Item)
		if len(item.Nodes) != prevReplFactor || prevReplFactor != r {
			modifiedShards = append(modifiedShards, item.Shard)
		}
		res[item.Shard] = item.Nodes
		count++
	}

	//add new shards if newNumShards > prevNumShards
	count = 0
	addedShards := []uint64{}

	//change random back after testing
	// random := rand.New(rand.NewSource(time.Now().UnixNano()))
	random := rand.New(rand.NewSource(1))
	for count < newNumShards-prevNumShards {
		shardID := (random.Uint64() % MaxHash)
		res[shardID] = []string{}
		addedShards = append(addedShards, shardID)
		count++
	}

	//remove shards if newNumShards < prevNumShards
	removedShards := []uint64{}
	for maxHeap.Len() > 0 {
		item := heap.Pop(&maxHeap).(*Item)
		removedShards = append(removedShards, item.Shard)
		modifiedShards = append(modifiedShards, item.Shard)
		for _, endpoint := range item.Nodes {
			movedNodes = append(movedNodes, endpoint)
		}
	}

	//make sure to add the new nodes to movedNodes as well
	for key, val := range storage {
		if val {
			movedNodes = append(movedNodes, key)
		}
	}

	//place nodes into shards with fewer than r amount of replicas
	v.PlaceNode(movedNodes, res, r)

	return res, addedShards, removedShards, modifiedShards
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

//genereate the position of a key in the hash space
func generateHash(key string) uint64 {
	hash := md5.Sum([]byte(key))
	bigInt := new(big.Int).SetBytes(hash[8:])
	return bigInt.Uint64() % MaxHash
}
