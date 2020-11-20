package kvs

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCalcNodeDiff(t *testing.T) {
	var tests = []struct {
		name    string
		v       View
		n       []string
		added   map[string]bool
		removed map[string]bool
	}{
		{"No diff", View{Nodes: []string{"1", "2", "3"}}, []string{"1", "2", "3"}, map[string]bool{}, map[string]bool{}},
		{"All added", View{Nodes: []string{}}, []string{"1", "2", "3"}, map[string]bool{"1": true, "2": true, "3": true}, map[string]bool{}},
		{"Nil view", View{}, []string{"1", "2", "3"}, map[string]bool{"1": true, "2": true, "3": true}, map[string]bool{}},
		{"All removed", View{Nodes: []string{"1", "2", "3"}}, []string{}, map[string]bool{}, map[string]bool{"1": true, "2": true, "3": true}},
		{"Some added", View{Nodes: []string{"1", "2", "4"}}, []string{"1", "2", "3", "4", "5"}, map[string]bool{"3": true, "5": true}, map[string]bool{}},
		{"Some removed", View{Nodes: []string{"1", "2", "3"}}, []string{"1"}, map[string]bool{}, map[string]bool{"2": true, "3": true}},
		{"Added and removed", View{Nodes: []string{"1", "2", "4"}}, []string{"3", "4", "5"}, map[string]bool{"3": true, "5": true}, map[string]bool{"1": true, "2": true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added, removed := tt.v.calcNodeDiff(tt.n)
			if !reflect.DeepEqual(added, tt.added) {
				t.Errorf("Want: %v Got: %v", added, tt.added)
			}

			if !reflect.DeepEqual(removed, tt.removed) {
				t.Errorf("Want: %v Got: %v", removed, tt.removed)
			}
		})
	}
}

func TestMergeTokens(t *testing.T) {
	var tests = []struct {
		name        string
		v           View
		newNodes    map[string]bool
		removeNodes map[string]bool
		addedTokens []Token
		tokenList   []Token
		changes     map[string]*Change
		collision   bool
	}{
		{"No change",
			View{Tokens: []Token{{Endpoint: "1", Value: 10}}},
			map[string]bool{},
			map[string]bool{},
			[]Token{},
			[]Token{{Endpoint: "1", Value: 10}},
			map[string]*Change{},
			false,
		},
		{"Initial",
			View{},
			map[string]bool{"1": true},
			map[string]bool{},
			[]Token{{Endpoint: "1", Value: 10}, {Endpoint: "1", Value: 20}, {Endpoint: "1", Value: 30}},
			[]Token{{Endpoint: "1", Value: 10}, {Endpoint: "1", Value: 20}, {Endpoint: "1", Value: 30}},
			map[string]*Change{"1": &Change{Tokens: []uint64{10, 20, 30}}},
			false,
		},
		{"Remove last node",
			View{Tokens: []Token{
				{Endpoint: "1", Value: 10},
				{Endpoint: "1", Value: 20},
				{Endpoint: "1", Value: 30}}},
			map[string]bool{},
			map[string]bool{"1": true},
			[]Token{},
			[]Token{},
			map[string]*Change{"1": &Change{Removed: true}},
			false,
		},
		{"Add 1 node",
			View{Tokens: []Token{
				{Endpoint: "3", Value: 10},
				{Endpoint: "1", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "3", Value: 25},
				{Endpoint: "1", Value: 30},
				{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{},
			[]Token{
				{Endpoint: "2", Value: 12},
				{Endpoint: "2", Value: 35},
				{Endpoint: "2", Value: 37},
			},
			[]Token{
				{Endpoint: "3", Value: 10},
				{Endpoint: "2", Value: 12},
				{Endpoint: "1", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "3", Value: 25},
				{Endpoint: "1", Value: 30},
				{Endpoint: "2", Value: 35},
				{Endpoint: "2", Value: 37},
				{Endpoint: "3", Value: 40},
			},
			map[string]*Change{
				"1": &Change{Tokens: []uint64{30}},
				"2": &Change{Tokens: []uint64{12, 35, 37}},
				"3": &Change{Tokens: []uint64{10}},
			},
			false,
		},
		{"Add node with token at start",
			View{Tokens: []Token{
				{Endpoint: "3", Value: 10},
				{Endpoint: "1", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "3", Value: 25},
				{Endpoint: "1", Value: 30},
				{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{},
			[]Token{
				{Endpoint: "2", Value: 2},
				{Endpoint: "2", Value: 35},
				{Endpoint: "2", Value: 37},
			},
			[]Token{
				{Endpoint: "2", Value: 2},
				{Endpoint: "3", Value: 10},
				{Endpoint: "1", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "3", Value: 25},
				{Endpoint: "1", Value: 30},
				{Endpoint: "2", Value: 35},
				{Endpoint: "2", Value: 37},
				{Endpoint: "3", Value: 40},
			},
			map[string]*Change{
				"1": &Change{Tokens: []uint64{30}},
				"2": &Change{Tokens: []uint64{2, 35, 37}},
				"3": &Change{Tokens: []uint64{40}},
			},
			false,
		},
		{"Add 1 nodes, remove 1",
			View{Tokens: []Token{
				{Endpoint: "3", Value: 10},
				{Endpoint: "1", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "3", Value: 25},
				{Endpoint: "1", Value: 30},
				{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{"3": true},
			[]Token{
				{Endpoint: "2", Value: 12},
				{Endpoint: "2", Value: 17},
				{Endpoint: "2", Value: 25},
			},
			[]Token{
				{Endpoint: "2", Value: 12},
				{Endpoint: "1", Value: 15},
				{Endpoint: "2", Value: 17},
				{Endpoint: "1", Value: 20},
				{Endpoint: "2", Value: 25},
				{Endpoint: "1", Value: 30},
			},
			map[string]*Change{
				"1": &Change{Tokens: []uint64{15, 20, 30}},
				"2": &Change{Tokens: []uint64{12, 17, 25}},
				"3": &Change{Removed: true},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, changes, collision := tt.v.mergeTokens(tt.addedTokens, tt.newNodes, tt.removeNodes)

			if !reflect.DeepEqual(tt.tokenList, tokens) {
				t.Errorf("Want: %v Got: %v", tt.tokenList, tokens)
			}

			if len(changes) != len(tt.changes) {
				t.Errorf("Changes should be: %v Got: %v", tt.changes, changes)
			} else {
				for i, v := range tt.changes {
					if !reflect.DeepEqual(v, changes[i]) {
						t.Errorf("Changes should be: %v Got: %v", v, changes[i])
						break
					}
				}
			}

			if tt.collision != collision {
				t.Errorf("Collision was %v when it shouldn't have been", collision)
			}
		})
	}
}

func TestBinarySearch(t *testing.T) {
	var tests = []structs {
		Tokens			[]Token
		expectedIndices []int
		target			[]uint64
	} {
		{
			[]Token{
				{Endpoint: "2", Value: 12},
				{Endpoint: "1", Value: 15},
				{Endpoint: "2", Value: 17},
				{Endpoint: "1", Value: 20},
				{Endpoint: "2", Value: 25},
				{Endpoint: "1", Value: 30},
			},
			[]int{
				1, 0, 0, 4, 4, 0,
			},
			[]uint64{
				12, 11, 39, 20, 24, 30
			}
		}
	}

	for _, test := range tests {
		for i := 0; i < len(target); i++ {
			endIndex := binarySearch(test.Tokens, test.target[i])
			if endIndex != test.expectedIndices[i] {
				t.Errorf("The index was %d when it should have been %d\n", endIndex, test.expectedIndices[i])
			}
		}
	}
}

func TestGenerateHash(t *testing.T) {
	key1 := "Surya"
	key2 := "Suresh"

	val1 := generateHash(key1)
	val2 := generateHash(key2)

	fmt.Println(val1, val2)

}
