package kvs

import (
	"reflect"
	"testing"
)

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
					{Shard: 1, Value: 1000},
					{Shard: 2, Value: 2000},
					{Shard: 3, Value: 3000},
					{Shard: 1, Value: 4000},
					{Shard: 2, Value: 5000},
					{Shard: 3, Value: 6000},
					{Shard: 1, Value: 7000},
					{Shard: 2, Value: 8000},
					{Shard: 3, Value: 9000},
					{Shard: 1, Value: 10000},
				},
			},
			[]Token{
				{Shard: 1, Value: 1000},
				{Shard: 1, Value: 1000},
				{Shard: 1, Value: 4000},
				{Shard: 2, Value: 8000},
				{Shard: 2, Value: 5000},
				{Shard: 2, Value: 5000},
				{Shard: 3, Value: 9000},
				{Shard: 3, Value: 9000},
				{Shard: 1, Value: 7000},
				{Shard: 3, Value: 6000},
			},
			[]string{
				"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9",
			},
		},
		{
			"testFindToken 2 with edge cases",
			View{
				Tokens: []Token{
					{Shard: 1, Value: 1000},
					{Shard: 2, Value: 2000},
					{Shard: 3, Value: 3000},
					{Shard: 1, Value: 4000},
					{Shard: 2, Value: 5000},
					{Shard: 3, Value: 6000},
					{Shard: 1, Value: 7000},
					{Shard: 2, Value: 8000},
					{Shard: 3, Value: 9000},
				},
			},
			[]Token{
				{Shard: 3, Value: 9000},
				{Shard: 2, Value: 5000},
				{Shard: 3, Value: 9000},
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
