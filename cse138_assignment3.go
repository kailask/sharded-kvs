package main

//Global constants for system
const (
	NumTokens = 5
	MaxHash   = 1024
)

type token struct {
	endpoint string
	value    uint16
}

type view struct {
	nodes  []string
	tokens []token
}

func (v *view) initTokens() {

}

func main() {
	// r := mux.NewRouter()
}
