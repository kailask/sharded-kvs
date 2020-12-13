package kvs

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Nodes    []string // The value of the item; arbitrary.
	Shard    uint64
	Priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	Index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

//Len function
func (pq PriorityQueue) Len() int { return len(pq) }

//Less function
func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Priority > pq[j].Priority
}

//Swap function
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

//Push function
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

//Pop function
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

//Update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, nodes []string, priority int, shard uint64) {
	item.Nodes = nodes
	item.Priority = priority
	item.Shard = shard
	heap.Fix(pq, item.Index)
}
