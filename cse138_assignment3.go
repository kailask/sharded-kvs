package main

import (
	"math/rand"
	"sort"
	"time"
)

//Global constants for system
const (
	NumTokens = 5
	MaxHash   = 1024
)

type token struct {
	endpoint string
	value    uint32
}

type view struct {
	nodes  []string
	tokens []token
}

type change struct {
	removed bool
	tokens  []uint32
}

//Register a change for a given token to a change map
func addChange(changes map[string]*change, t *token) {
	if c, exists := changes[t.endpoint]; exists {
		c.tokens = append(c.tokens, t.value)
	} else {
		changes[t.endpoint] = &change{tokens: []uint32{t.value}}
	}
}

//Generate list of random tokens given map of nodes to add
func generateTokens(addedNodes map[string]bool) []token {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := make([]token, len(addedNodes)*NumTokens)

	for node := range addedNodes {
		for i := 0; i < NumTokens; i++ {
			tokens = append(tokens, token{endpoint: node, value: r.Uint32() % MaxHash})
		}
	}

	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].value < tokens[j].value
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

	v.nodes = nodes
	v.tokens = tokens
	return changes
}

//Calculate the added and removed nodes as differences between the view and a given node list
func (v *view) calcNodeDiff(nodes []string) (map[string]bool, map[string]bool) {
	addedNodes, removedNodes := make(map[string]bool), make(map[string]bool)
	nodesMap := make(map[string]int, len(nodes))

	for _, node := range v.nodes {
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
	newLength := len(v.tokens) + len(addedTokens) - (len(removedNodes) * NumTokens)
	changes := make(map[string]*change)
	tokens := make([]token, newLength)

	lastWasChanged := false
	vIndex, aIndex := 0, 0
	var vToken, aToken *token

	if v.tokens != nil && len(v.tokens) > 0 {
		vToken = &v.tokens[vIndex]
	}

	if len(addedTokens) > 0 {
		aToken = &addedTokens[aIndex]
	}

	//Iterate through new tokens list each time adding smallest token from added tokens or view tokens
	for i := 0; aToken != nil && vToken != nil; i++ {
		if vToken != nil {
			//Iterate through view tokens until a token is found that isn't being removed
			for removedNodes[vToken.endpoint] {
				//Register change for removed token
				if _, exists := changes[vToken.endpoint]; !exists {
					changes[vToken.endpoint] = &change{removed: true}
				}

				vIndex++
				if vIndex < len(v.tokens) {
					vToken = &v.tokens[vIndex]
				} else {
					vToken = nil
					break
				}
			}
		}

		//Add smallest token to new list at i from added tokens or view tokens
		if aToken != nil && (vToken == nil || aToken.value <= vToken.value) {
			//Detect collisions either between current added and view tokens or current and prev added tokens
			if (vToken != nil && aToken.value == vToken.value) || (aIndex > 0 && aToken.value == addedTokens[aIndex-1].value) {
				return nil, nil, true
			}

			//Register change for added token
			addChange(changes, aToken)

			//Register change for view token changed by added token
			if i == 0 {
				lastWasChanged = true
			} else if !addedNodes[tokens[i-1].endpoint] {
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
			tokens[i] = *vToken
			vIndex++

			//If we just added the last token to the new list we must notify it if lastWasChanged
			if lastWasChanged && i == len(tokens) {
				addChange(changes, vToken)
			}

			if vIndex < len(v.tokens) {
				vToken = &v.tokens[vIndex]
			} else {
				vToken = nil
			}
		}
	}

	return tokens, changes, false
}

func main() {
	// r := mux.NewRouter()
}
