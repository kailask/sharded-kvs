package kvs

import (
	"reflect"
	"sort"
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

func TestFindToken(t *testing.T) {
	// this test uses the following config
	// const (
	// 	NumTokens = 2
	// 	MaxHash   = 10000
	// )
	var tests = []struct {
		name           string
		view           View
		expectedTokens []Token
		keys           []string
	}{
		{
			"testFindToken 1",
			View{
				Tokens: []Token{
					{Endpoint: "1", Value: 1000},
					{Endpoint: "2", Value: 2000},
					{Endpoint: "3", Value: 3000},
					{Endpoint: "1", Value: 4000},
					{Endpoint: "2", Value: 5000},
					{Endpoint: "3", Value: 6000},
					{Endpoint: "1", Value: 7000},
					{Endpoint: "2", Value: 8000},
					{Endpoint: "3", Value: 9000},
					{Endpoint: "1", Value: 10000},
				},
			},
			[]Token{
				{Endpoint: "1", Value: 1000},
				{Endpoint: "1", Value: 1000},
				{Endpoint: "1", Value: 4000},
				{Endpoint: "2", Value: 8000},
				{Endpoint: "2", Value: 5000},
				{Endpoint: "2", Value: 5000},
				{Endpoint: "3", Value: 9000},
				{Endpoint: "3", Value: 9000},
				{Endpoint: "1", Value: 7000},
				{Endpoint: "3", Value: 6000},
			},
			[]string{
				"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9",
			},
		},
		{
			"testFindToken 2 with edge cases",
			View{
				Tokens: []Token{
					{Endpoint: "1", Value: 1000},
					{Endpoint: "2", Value: 2000},
					{Endpoint: "3", Value: 3000},
					{Endpoint: "1", Value: 4000},
					{Endpoint: "2", Value: 5000},
					{Endpoint: "3", Value: 6000},
					{Endpoint: "1", Value: 7000},
					{Endpoint: "2", Value: 8000},
					{Endpoint: "3", Value: 9000},
				},
			},
			[]Token{
				{Endpoint: "3", Value: 9000},
				{Endpoint: "2", Value: 5000},
				{Endpoint: "3", Value: 9000},
			},
			[]string{
				"2740103009342231109", "9139560586737125025", "605394647632969758",
			},
		},
	}

	for _, test := range tests {
		for i := 0; i < len(test.expectedTokens); i++ {
			actualToken := test.view.FindToken(test.keys[i])
			if !reflect.DeepEqual(actualToken, test.expectedTokens[i]) {
				t.Errorf("%s Token should be %v, got %v\n", test.name, test.expectedTokens[i], actualToken)
			}
		}
	}
}

func TestCreateShardList(t *testing.T) {
	testCases := []struct {
		desc                   string
		view                   View
		nodes                  []string
		r                      int
		expectedShardsList     map[uint64][]string
		expectedShardsRemoved  []uint64
		expectedShardsAdded    []uint64
		expectedShardsModified []uint64
	}{
		{
			"test 1 shard 69 keeps both its nodes",
			View{
				Nodes: []string{
					"1", "2", "3", "4", "5", "6",
				},
				ShardsList: map[uint64][]string{
					37: {
						"1", "2", "3",
					},
					69: {
						"4", "5", "6",
					},
				},
			},
			[]string{"1", "4", "5", "7"},
			2,
			map[uint64][]string{
				37: {
					"1", "7",
				},
				69: {
					"4", "5",
				},
			},
			[]uint64{},
			[]uint64{},
			[]uint64{37, 69},
		},
		{
			"test 2 shard 37 is the only one left",
			View{
				Nodes: []string{
					"1", "2", "3", "4", "5", "6",
				},
				ShardsList: map[uint64][]string{
					37: {
						"1",
					},
					69: {
						"2",
					},
					72: {
						"3",
					},
					83: {
						"4",
					},
					95: {
						"5",
					},
					101: {
						"6",
					},
				},
			},
			[]string{"1", "4", "5", "7"},
			4,
			map[uint64][]string{
				95: {
					"1", "4", "5", "7",
				},
			},
			[]uint64{37, 69, 72, 83, 101},
			[]uint64{},
			[]uint64{37, 69, 72, 83, 95, 101},
		},
		{
			"test 3 to see if new shards are created during view change",
			View{
				Nodes: []string{
					"1", "2", "3", "4", "5", "6",
				},
				ShardsList: map[uint64][]string{
					54: {
						"1", "2", "3", "4", "5", "6",
					},
				},
			},
			[]string{
				"1", "2", "5", "6", "7", "8",
			},
			2,
			map[uint64][]string{
				54: {
					"1", "2",
				},
				779410: {
					"5", "6",
				},
				153551: {
					"7", "8",
				},
			},
			[]uint64{},
			[]uint64{153551, 779410},
			[]uint64{54},
		},
		{
			"test 4 the most complicated test I could come up with",
			View{
				Nodes: []string{
					"1", "2", "3", "4", "5", "6",
				},
				ShardsList: map[uint64][]string{
					54: {
						"1", "6",
					},
					23: {
						"2", "4",
					},
					79: {
						"5", "3",
					},
				},
			},
			[]string{
				"2", "4", "9", "11",
			},
			1,
			map[uint64][]string{
				23: {
					"2",
				},
				54: {
					"4",
				},
				79: {
					"9",
				},
				779410: {
					"11",
				},
			},
			[]uint64{},
			[]uint64{779410},
			[]uint64{23, 54, 79},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			actualShardsList, added, removed, modified := tC.view.CreateShardList(tC.nodes, tC.r)
			sort.SliceStable(added, func(i, j int) bool { return added[i] < added[j] })
			sort.SliceStable(removed, func(i, j int) bool { return removed[i] < removed[j] })
			sort.SliceStable(modified, func(i, j int) bool { return modified[i] < modified[j] })
			if !reflect.DeepEqual(tC.expectedShardsList, actualShardsList) {
				t.Errorf("%s Shards list should be %v, got %v\n", tC.desc, tC.expectedShardsList, actualShardsList)
			}
			if !reflect.DeepEqual(tC.expectedShardsAdded, added) {
				t.Errorf("%s added shards should be %v, got %v\n", tC.desc, tC.expectedShardsAdded, added)
			}
			if !reflect.DeepEqual(tC.expectedShardsRemoved, removed) {
				t.Errorf("%s removed shards should be %v, got %v\n", tC.desc, tC.expectedShardsRemoved, removed)
			}
			if !reflect.DeepEqual(tC.expectedShardsModified, modified) {
				t.Errorf("%s modified shards should be %v, got %v\n", tC.desc, tC.expectedShardsModified, modified)
			}
		})
	}
}

func TestReshard(t *testing.T) {
	var testCases = []struct {
		desc              string
		paritionedKVS     map[uint64]map[string]string
		expectedRemapping map[string]map[string]map[string]string
		view              View
		change            Change

		// partitions = PartionedKVS,

	}{
		{
			desc: "",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {

		})
	}
}

// func TestReshard(t *testing.T) {
// 	// //initialize KVS map for node 1
// 	// for i := 0; i < 20; i++ {
// 	// 	s := strconv.Itoa(i)
// 	// 	num := generateHash("key" + s)

// 	// 	if 100 <= num && num < 309 {
// 	// 		_, exists := KVS[100]
// 	// 		if exists {
// 	// 			KVS[100]["key"+s] = s
// 	// 		} else {
// 	// 			kvs := make(map[string]string)
// 	// 			KVS[100] = kvs
// 	// 			KVS[100]["key"+s] = s
// 	// 		}
// 	// 	}

// 	// 	if 490 <= num && num < 854 {
// 	// 		_, exists := KVS[490]
// 	// 		if exists {
// 	// 			KVS[490]["key"+s] = s
// 	// 		} else {
// 	// 			kvs := make(map[string]string)
// 	// 			KVS[490] = kvs
// 	// 			KVS[490]["key"+s] = s
// 	// 		}
// 	// 	}

// 	// 	if num >= 934 || num < 100 {
// 	// 		_, exists := KVS[934]
// 	// 		if exists {
// 	// 			KVS[934]["key"+s] = s
// 	// 		} else {
// 	// 			kvs := make(map[string]string)
// 	// 			KVS[934] = kvs
// 	// 			KVS[934]["key"+s] = s
// 	// 		}
// 	// 	}
// 	// }

// 	// pos := generateHash("Surya")
// 	// if pos != 0 {
// 	// 	t.Error("\nKVS is:", KVS)
// 	// }

// 	// //create the expected map of repartitions
// 	// expectedMap := make(map[string]map[string]map[string]string)
// 	// for key, value := range KVS {
// 	// 	if key == 100 {
// 	// 		for k, v := range value {
// 	// 			if generateHash(k) >= 223 {
// 	// 				_, exists := expectedMap["2"]
// 	// 				if exists {
// 	// 					_, ex := expectedMap["2"][strconv.Itoa(223)]
// 	// 					if ex {
// 	// 						expectedMap["2"][strconv.Itoa(223)][k] = v
// 	// 					} else {
// 	// 						kvs := make(map[string]string)
// 	// 						expectedMap["2"][strconv.Itoa(223)] = kvs
// 	// 						expectedMap["2"][strconv.Itoa(223)][k] = v
// 	// 					}
// 	// 				} else {
// 	// 					kvs := make(map[string]string)
// 	// 					node := make(map[string]map[string]string)
// 	// 					expectedMap["2"] = node
// 	// 					expectedMap["2"][strconv.Itoa(223)] = kvs
// 	// 					expectedMap["2"][strconv.Itoa(223)][k] = v
// 	// 				}
// 	// 			}
// 	// 		}
// 	// 	}
// 	// 	if key == 490 {
// 	// 		for k, v := range value {
// 	// 			if generateHash(k) >= 670 {
// 	// 				_, exists := expectedMap["2"]
// 	// 				if exists {
// 	// 					_, ex := expectedMap["2"][strconv.Itoa(670)]
// 	// 					if ex {
// 	// 						expectedMap["2"][strconv.Itoa(670)][k] = v
// 	// 					} else {
// 	// 						kvs := make(map[string]string)
// 	// 						expectedMap["2"][strconv.Itoa(670)] = kvs
// 	// 						expectedMap["2"][strconv.Itoa(670)][k] = v
// 	// 					}
// 	// 				} else {
// 	// 					kvs := make(map[string]string)
// 	// 					node := make(map[string]map[string]string)
// 	// 					expectedMap["2"] = node
// 	// 					expectedMap["2"][strconv.Itoa(670)] = kvs
// 	// 					expectedMap["2"][strconv.Itoa(670)][k] = v
// 	// 				}
// 	// 			}
// 	// 		}
// 	// 	}
// 	// 	if key == 934 {
// 	// 		for k, v := range value {
// 	// 			if generateHash(k) >= 1000 || generateHash(k) < 100 {
// 	// 				_, exists := expectedMap["2"]
// 	// 				if exists {
// 	// 					_, ex := expectedMap["2"][strconv.Itoa(1000)]
// 	// 					if ex {
// 	// 						expectedMap["2"][strconv.Itoa(1000)][k] = v
// 	// 					} else {
// 	// 						kvs := make(map[string]string)
// 	// 						expectedMap["2"][strconv.Itoa(1000)] = kvs
// 	// 						expectedMap["2"][strconv.Itoa(1000)][k] = v
// 	// 					}
// 	// 				} else {
// 	// 					kvs := make(map[string]string)
// 	// 					node := make(map[string]map[string]string)
// 	// 					expectedMap["2"] = node
// 	// 					expectedMap["2"][strconv.Itoa(1000)] = kvs
// 	// 					expectedMap["2"][strconv.Itoa(1000)][k] = v
// 	// 				}
// 	// 			}
// 	// 		}
// 	// 	}
// 	// }

// 	// pos = generateHash("Surya")
// 	// if pos != 0 {
// 	// 	t.Error("\nexpectedMap is:", expectedMap, "\nexpected KVS is:", KVS)
// 	// }

// 	//initialize KVS map for node 2
// 	for i := 0; i < 20; i++ {
// 		s := strconv.Itoa(i)
// 		num := generateHash("key" + s)

// 		if 223 <= num && num < 309 {
// 			_, exists := KVS[223]
// 			if exists {
// 				KVS[223]["key"+s] = s
// 			} else {
// 				kvs := make(map[string]string)
// 				KVS[223] = kvs
// 				KVS[223]["key"+s] = s
// 			}
// 		}

// 		if 670 <= num && num < 854 {
// 			_, exists := KVS[670]
// 			if exists {
// 				KVS[670]["key"+s] = s
// 			} else {
// 				kvs := make(map[string]string)
// 				KVS[670] = kvs
// 				KVS[670]["key"+s] = s
// 			}
// 		}

// 		if num >= 1000 || num < 100 {
// 			_, exists := KVS[1000]
// 			if exists {
// 				KVS[1000]["key"+s] = s
// 			} else {
// 				kvs := make(map[string]string)
// 				KVS[1000] = kvs
// 				KVS[1000]["key"+s] = s
// 			}
// 		}
// 	}

// 	// pos := generateHash("Surya")
// 	// if pos != 0 {
// 	// 	t.Error("\nKVS is:", KVS)
// 	// }

// 	expectedMap2 := map[string]map[string]map[string]string{
// 		"1": {
// 			"490": {
// 				"key1":  "1",
// 				"key12": "12",
// 				"key2":  "2",
// 				"key7":  "7",
// 				"key8":  "8",
// 			},
// 			"934": {
// 				"key13": "13",
// 				"key17": "17",
// 				"key18": "18",
// 				"key5":  "5",
// 			},
// 		},
// 	}

// 	//TODO: create another test that removes node 2 from the system
// 	var tests = []struct {
// 		name   string
// 		v      View
// 		change Change
// 		// expectedMap 	map[string]map[string]map[string][string] why is it complaining?
// 	}{
// 		// {
// 		// 	"Reshard test 1, 2 got added",
// 		// 	View{
// 		// 		Tokens: []Token{
// 		// 			{Endpoint: "1", Value: 100},
// 		// 			{Endpoint: "2", Value: 223},
// 		// 			{Endpoint: "3", Value: 309},
// 		// 			{Endpoint: "1", Value: 490},
// 		// 			{Endpoint: "2", Value: 670},
// 		// 			{Endpoint: "3", Value: 854},
// 		// 			{Endpoint: "1", Value: 934},
// 		// 			{Endpoint: "2", Value: 1000},
// 		// 		},
// 		// 	},
// 		// 	Change{Removed: false, Tokens: []uint64{100, 490, 934}},
// 		// 	// nil,
// 		// },

// 		{
// 			"Reshard test 2, 2 got removed",
// 			View{
// 				Tokens: []Token{
// 					{Endpoint: "1", Value: 100},
// 					{Endpoint: "3", Value: 309},
// 					{Endpoint: "1", Value: 490},
// 					{Endpoint: "3", Value: 854},
// 					{Endpoint: "1", Value: 934},
// 				},
// 			},
// 			Change{Removed: true},
// 		},
// 	}

// 	for _, test := range tests {
// 		res := test.v.Reshard(test.change)
// 		if reflect.DeepEqual(res, expectedMap2) {
// 			t.Errorf("%s\n The map was supposed to be %v but got %v\n new KVS is %v\n", test.name, expectedMap2, res, KVS)
// 		}
// 	}

// }
