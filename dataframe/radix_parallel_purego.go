//go:build purego
// +build purego

package dataframe

// ParallelRadixSortUint64 is a stub for purego builds that defers to serial radix.
func ParallelRadixSortUint64(keys []uint64, ascending bool) []int {
	return radixSortUint64Keys(keys, ascending)
}
