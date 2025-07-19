//go:build purego
// +build purego

package dataframe

import "sort"

// radixSortInt64 provides a simple fallback implementation that uses sort.Slice
// when the purego build tag is requested. This keeps the API identical to the
// high-performance version while avoiding unsafe or architecture-specific code.
func radixSortInt64(data []int64, ascending bool) []int {
	indices := make([]int, len(data))
	for i := range indices {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		if ascending {
			return data[indices[i]] < data[indices[j]]
		}
		return data[indices[i]] > data[indices[j]]
	})

	return indices
}

// radixSortFloat64 is the float64 counterpart of radixSortInt64 for the purego
// build. It reuses the standard library's sort.Slice implementation.
func radixSortFloat64(data []float64, ascending bool) []int {
	indices := make([]int, len(data))
	for i := range indices {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		if ascending {
			return data[indices[i]] < data[indices[j]]
		}
		return data[indices[i]] > data[indices[j]]
	})

	return indices
}
