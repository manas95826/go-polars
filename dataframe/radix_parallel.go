//go:build !purego
// +build !purego

package dataframe

import (
	"runtime"
)

// helper types for k-way merge
type radixNode struct {
	key   uint64
	idx   int
	shard int
}

type radixMergeHeap struct {
	ascending bool
	nodes     []radixNode
}

func (h radixMergeHeap) Len() int { return len(h.nodes) }
func (h radixMergeHeap) Less(i, j int) bool {
	if h.ascending {
		return h.nodes[i].key < h.nodes[j].key
	}
	return h.nodes[i].key > h.nodes[j].key
}

func (h radixMergeHeap) Swap(i, j int) { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }

func (h *radixMergeHeap) Push(x interface{}) { h.nodes = append(h.nodes, x.(radixNode)) }

func (h *radixMergeHeap) Pop() interface{} {
	old := h.nodes
	n := len(old)
	x := old[n-1]
	h.nodes = old[:n-1]
	return x
}

// ParallelRadixSortUint64 sorts keys (and returns indices) using parallel sharding.
// It is stable and asc/desc is determined by ascending flag.
func ParallelRadixSortUint64(keys []uint64, ascending bool) []int {
	n := len(keys)
	if n <= 1 {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		return idx
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 2 || n < 1<<15 { // fall back to serial for small workloads
		return radixSortUint64Keys(keys, ascending)
	}

	// Use new in-place parallel radix implementation
	return radixSortUint64KeysParallel(keys, ascending)
}
