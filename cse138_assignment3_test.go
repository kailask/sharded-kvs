package main

import (
	"reflect"
	"testing"
)

func TestCalcNodeDiff(t *testing.T) {
	var tests = []struct {
		name    string
		v       view
		n       []string
		added   map[string]bool
		removed map[string]bool
	}{
		{"No diff", view{Nodes: []string{"1", "2", "3"}}, []string{"1", "2", "3"}, map[string]bool{}, map[string]bool{}},
		{"All added", view{Nodes: []string{}}, []string{"1", "2", "3"}, map[string]bool{"1": true, "2": true, "3": true}, map[string]bool{}},
		{"Nil view", view{}, []string{"1", "2", "3"}, map[string]bool{"1": true, "2": true, "3": true}, map[string]bool{}},
		{"All removed", view{Nodes: []string{"1", "2", "3"}}, []string{}, map[string]bool{}, map[string]bool{"1": true, "2": true, "3": true}},
		{"Some added", view{Nodes: []string{"1", "2", "4"}}, []string{"1", "2", "3", "4", "5"}, map[string]bool{"3": true, "5": true}, map[string]bool{}},
		{"Some removed", view{Nodes: []string{"1", "2", "3"}}, []string{"1"}, map[string]bool{}, map[string]bool{"2": true, "3": true}},
		{"Added and removed", view{Nodes: []string{"1", "2", "4"}}, []string{"3", "4", "5"}, map[string]bool{"3": true, "5": true}, map[string]bool{"1": true, "2": true}},
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
		v           view
		newNodes    map[string]bool
		removeNodes map[string]bool
		addedTokens []token
		tokenList   []token
		changes     map[string]*change
		collision   bool
	}{
		{"No change",
			view{Tokens: []token{token{Endpoint: "1", Value: 10}}},
			map[string]bool{},
			map[string]bool{},
			[]token{},
			[]token{token{Endpoint: "1", Value: 10}},
			map[string]*change{},
			false,
		},
		{"Initial",
			view{},
			map[string]bool{"1": true},
			map[string]bool{},
			[]token{token{Endpoint: "1", Value: 10}, token{Endpoint: "1", Value: 20}, token{Endpoint: "1", Value: 30}},
			[]token{token{Endpoint: "1", Value: 10}, token{Endpoint: "1", Value: 20}, token{Endpoint: "1", Value: 30}},
			map[string]*change{"1": &change{Tokens: []uint32{10, 20, 30}}},
			false,
		},
		{"Remove last node",
			view{Tokens: []token{
				token{Endpoint: "1", Value: 10},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "1", Value: 30}}},
			map[string]bool{},
			map[string]bool{"1": true},
			[]token{},
			[]token{},
			map[string]*change{"1": &change{Removed: true}},
			false,
		},
		{"Add 1 node",
			view{Tokens: []token{
				token{Endpoint: "3", Value: 10},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "3", Value: 25},
				token{Endpoint: "1", Value: 30},
				token{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{},
			[]token{
				token{Endpoint: "2", Value: 12},
				token{Endpoint: "2", Value: 35},
				token{Endpoint: "2", Value: 37},
			},
			[]token{
				token{Endpoint: "3", Value: 10},
				token{Endpoint: "2", Value: 12},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "3", Value: 25},
				token{Endpoint: "1", Value: 30},
				token{Endpoint: "2", Value: 35},
				token{Endpoint: "2", Value: 37},
				token{Endpoint: "3", Value: 40},
			},
			map[string]*change{
				"1": &change{Tokens: []uint32{30}},
				"2": &change{Tokens: []uint32{12, 35, 37}},
				"3": &change{Tokens: []uint32{10}},
			},
			false,
		},
		{"Add node with token at start",
			view{Tokens: []token{
				token{Endpoint: "3", Value: 10},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "3", Value: 25},
				token{Endpoint: "1", Value: 30},
				token{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{},
			[]token{
				token{Endpoint: "2", Value: 2},
				token{Endpoint: "2", Value: 35},
				token{Endpoint: "2", Value: 37},
			},
			[]token{
				token{Endpoint: "2", Value: 2},
				token{Endpoint: "3", Value: 10},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "3", Value: 25},
				token{Endpoint: "1", Value: 30},
				token{Endpoint: "2", Value: 35},
				token{Endpoint: "2", Value: 37},
				token{Endpoint: "3", Value: 40},
			},
			map[string]*change{
				"1": &change{Tokens: []uint32{30}},
				"2": &change{Tokens: []uint32{2, 35, 37}},
				"3": &change{Tokens: []uint32{40}},
			},
			false,
		},
		{"Add 1 nodes, remove 1",
			view{Tokens: []token{
				token{Endpoint: "3", Value: 10},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "3", Value: 25},
				token{Endpoint: "1", Value: 30},
				token{Endpoint: "3", Value: 40},
			}},
			map[string]bool{"2": true},
			map[string]bool{"3": true},
			[]token{
				token{Endpoint: "2", Value: 12},
				token{Endpoint: "2", Value: 17},
				token{Endpoint: "2", Value: 25},
			},
			[]token{
				token{Endpoint: "2", Value: 12},
				token{Endpoint: "1", Value: 15},
				token{Endpoint: "2", Value: 17},
				token{Endpoint: "1", Value: 20},
				token{Endpoint: "2", Value: 25},
				token{Endpoint: "1", Value: 30},
			},
			map[string]*change{
				"1": &change{Tokens: []uint32{15, 20, 30}},
				"2": &change{Tokens: []uint32{12, 17, 25}},
				"3": &change{Removed: true},
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
