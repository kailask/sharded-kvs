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

//KVS is a string:string key value store
type KVS map[string]string

//PartitionedKVS is a string:string kvs divided into a map of partitions
type PartitionedKVS map[uint64]KVS

//MyKVS maps token values to k:v maps
var MyKVS = PartitionedKVS{}

//Global constants for kvs
const (
	NumTokens = 5
	MaxHash   = 100
)

//Token contains an ip address and value in has space
type Token struct {
	Shard int    `json:"shard"`
	Value uint64 `json:"value"`
}

//View contains list of current nodes and their sorted tokens
type View struct {
	Nodes  []string `json:"nodes"`
	Tokens []Token  `json:"tokens"`
	Shards map[int][]string
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

//GetShardID return ID of shard
func (v *View) GetShardID(endpoint string) string {
	for id, nodes := range v.Shards {
		for _, node := range nodes {
			if node == endpoint {
				return strconv.Itoa(id)
			}
		}
	}
	return ""
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

//SetTokens sets the tokens
func SetTokens(tokens []Token) {
	for _, token := range tokens {
		MyKVS[token.Value] = make(KVS)
	}
}

//GenerateView changes view struct given new state of active nodes. Returns map of changes and map of new nodes
func (v *View) GenerateView(nodes []string, repl int) map[string][]Token {
	numShards := len(nodes) / repl
	tokensMap, shardTokens := generateTokens(numShards)

	shards := make(map[int][]string)
	for i := 0; i < len(nodes); i++ {
		shardNum := i % numShards
		if n, exists := shards[shardNum]; exists {
			shards[shardNum] = append(n, nodes[i])
		} else {
			shards[shardNum] = []string{nodes[i]}
		}
	}

	tokens := []Token{}
	for _, v := range tokensMap {
		tokens = append(tokens, v)
	}

	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].Value < tokens[j].Value
	})

	nodeTokens := make(map[string][]Token)
	for k, v := range shardTokens {
		for _, node := range shards[k] {
			nodeTokens[node] = v
		}
	}

	v.Nodes = nodes
	v.Tokens = tokens
	v.Shards = shards
	return nodeTokens
}

//Generate list of random tokens given map of nodes to add
func generateTokens(numShards int) (map[uint64]Token, map[int][]Token) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := make(map[uint64]Token)
	shardTokens := make(map[int][]Token)

	for shard := 0; shard < numShards; shard++ {
		shardTokens[shard] = []Token{}
		for i := 0; i < NumTokens; i++ {
			t := Token{Shard: shard, Value: r.Uint64() % MaxHash}
			if _, exists := tokens[t.Value]; exists {
				i--
			} else {
				tokens[t.Value] = t
				shardTokens[shard] = append(shardTokens[shard], t)
			}
		}
	}

	return tokens, shardTokens
}

//genereate the position of a key in the hash space
func generateHash(key string) uint64 {
	hash := md5.Sum([]byte(key))
	bigInt := new(big.Int).SetBytes(hash[8:])
	return bigInt.Uint64() % MaxHash
}
