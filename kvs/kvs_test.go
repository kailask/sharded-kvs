package kvs

import (
	"reflect"
	"strconv"
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
			map[string]*Change{"1": {Tokens: []uint64{10, 20, 30}}},
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
			map[string]*Change{"1": {Removed: true}},
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
				"1": {Tokens: []uint64{30}},
				"2": {Tokens: []uint64{12, 35, 37}},
				"3": {Tokens: []uint64{10}},
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
				"1": {Tokens: []uint64{30}},
				"2": {Tokens: []uint64{2, 35, 37}},
				"3": {Tokens: []uint64{40}},
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
				"1": {Tokens: []uint64{15, 20, 30}},
				"2": {Tokens: []uint64{12, 17, 25}},
				"3": {Removed: true},
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
	var tests = []struct {
		name            string
		Tokens          []Token
		expectedIndices []int
		target          []uint64
	}{
		{
			"Binary search test 1",
			[]Token{
				{Endpoint: "2", Value: 12},
				{Endpoint: "1", Value: 15},
				{Endpoint: "2", Value: 17},
				{Endpoint: "1", Value: 20},
				{Endpoint: "2", Value: 25},
				{Endpoint: "1", Value: 30},
			},
			[]int{
				0, 5, 5, 3, 3, 5,
			},
			[]uint64{
				12, 11, 39, 20, 24, 30,
			},
		},
		{
			"Binary search test 2",
			[]Token{
				{Endpoint: "1", Value: 5},
				{Endpoint: "2", Value: 15},
				{Endpoint: "1", Value: 20},
				{Endpoint: "2", Value: 50},
			},
			[]int{
				0, 3, 3, 0, 2,
			},
			[]uint64{
				11, 100, 50, 5, 20,
			},
		},
	}

	for _, test := range tests {
		for i := 0; i < len(test.target); i++ {
			endIndex := binarySearch(test.Tokens, test.target[i])
			if endIndex != test.expectedIndices[i] {
				t.Errorf("%s The index was %d when it should have been %d\n", test.name, endIndex, test.expectedIndices[i])
			}
		}
	}
}

func TestReshard(t *testing.T) {
	//initialize KVS map
	for i := 0; i < 20; i++ {
		s := strconv.Itoa(i)
		num := generateHash("key" + s)

		if 100 <= num && num < 309 {
			_, exists := KVS[100]
			if exists {
				KVS[100]["key"+s] = s
			} else {
				kvs := make(map[string]string)
				KVS[100] = kvs
				KVS[100]["key"+s] = s
			}
		}

		if 490 <= num && num < 854 {
			_, exists := KVS[490]
			if exists {
				KVS[490]["key"+s] = s
			} else {
				kvs := make(map[string]string)
				KVS[490] = kvs
				KVS[490]["key"+s] = s
			}
		}

		if num >= 934 || num < 100 {
			_, exists := KVS[934]
			if exists {
				KVS[934]["key"+s] = s
			} else {
				kvs := make(map[string]string)
				KVS[934] = kvs
				KVS[934]["key"+s] = s
			}
		}
	}

	// pos := generateHash("Surya")
	// if pos != 0 {
	// 	t.Error("\nKVS is:", KVS)
	// }

	//create the expected map of repartitions
	expectedMap := make(map[string]map[string]map[string]string)
	for key, value := range KVS {
		if key == 100 {
			for k, v := range value {
				if generateHash(k) >= 223 {
					_, exists := expectedMap["2"]
					if exists {
						_, ex := expectedMap["2"][strconv.Itoa(223)]
						if ex {
							expectedMap["2"][strconv.Itoa(223)][k] = v
						} else {
							kvs := make(map[string]string)
							expectedMap["2"][strconv.Itoa(223)] = kvs
							expectedMap["2"][strconv.Itoa(223)][k] = v
						}
					} else {
						kvs := make(map[string]string)
						node := make(map[string]map[string]string)
						expectedMap["2"] = node
						expectedMap["2"][strconv.Itoa(223)] = kvs
						expectedMap["2"][strconv.Itoa(223)][k] = v
					}
				}
			}
		}
		if key == 490 {
			for k, v := range value {
				if generateHash(k) >= 670 {
					_, exists := expectedMap["2"]
					if exists {
						_, ex := expectedMap["2"][strconv.Itoa(670)]
						if ex {
							expectedMap["2"][strconv.Itoa(670)][k] = v
						} else {
							kvs := make(map[string]string)
							expectedMap["2"][strconv.Itoa(670)] = kvs
							expectedMap["2"][strconv.Itoa(670)][k] = v
						}
					} else {
						kvs := make(map[string]string)
						node := make(map[string]map[string]string)
						expectedMap["2"] = node
						expectedMap["2"][strconv.Itoa(670)] = kvs
						expectedMap["2"][strconv.Itoa(670)][k] = v
					}
				}
			}
		}
		if key == 934 {
			for k, v := range value {
				if generateHash(k) >= 1000 || generateHash(k) < 100 {
					_, exists := expectedMap["2"]
					if exists {
						_, ex := expectedMap["2"][strconv.Itoa(1000)]
						if ex {
							expectedMap["2"][strconv.Itoa(1000)][k] = v
						} else {
							kvs := make(map[string]string)
							expectedMap["2"][strconv.Itoa(1000)] = kvs
							expectedMap["2"][strconv.Itoa(1000)][k] = v
						}
					} else {
						kvs := make(map[string]string)
						node := make(map[string]map[string]string)
						expectedMap["2"] = node
						expectedMap["2"][strconv.Itoa(1000)] = kvs
						expectedMap["2"][strconv.Itoa(1000)][k] = v
					}
				}
			}
		}
	}

	// pos := generateHash("Surya")
	// if pos != 0 {
	// 	t.Error("\nexpectedMap is:", expectedMap)
	// }

	var tests = []struct {
		name   string
		v      View
		change Change
	}{
		{
			"Reshard test 1, 2 got added",
			View{
				Tokens: []Token{
					{Endpoint: "1", Value: 100},
					{Endpoint: "2", Value: 223},
					{Endpoint: "3", Value: 309},
					{Endpoint: "1", Value: 490},
					{Endpoint: "2", Value: 670},
					{Endpoint: "3", Value: 854},
					{Endpoint: "1", Value: 934},
					{Endpoint: "2", Value: 1000},
				},
			},
			Change{Removed: false, Tokens: []uint64{100, 490, 934}},
		},
	}

	for _, test := range tests {
		res := test.v.Reshard(test.change)
		if !reflect.DeepEqual(res, expectedMap) {
			t.Errorf("%s\n The map was supposed to be %v but got %v\n", test.name, expectedMap, res)
		}
	}

}
