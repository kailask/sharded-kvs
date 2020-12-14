package kvs

import (
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
	Nodes  []string `json:"nodes"`
	Tokens []Token  `json:"tokens"`
	Shards map[uint64][]string
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
