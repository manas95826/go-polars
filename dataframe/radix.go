//go:build !purego
// +build !purego

package dataframe

import "math"

// radixSortInt64 performs an unsigned radix sort on int64 data and returns a slice
// of indices that represent the order of the sorted data. The sort is performed
// in O(64/b) passes where b=8 (one byte per pass) and is stable.
// Negative numbers are handled by XOR biased conversion to uint64.
func radixSortInt64(data []int64, ascending bool) []int {
	n := len(data)
	if n == 0 {
		return nil
	}

	// Convert signed values to biased unsigned keys so that the lexicographic
	// order of the keys matches the desired numeric order.
	keys := make([]uint64, n)
	for i, v := range data {
		keys[i] = uint64(v) ^ 0x8000000000000000
	}

	return radixSortUint64Keys(keys, ascending)
}

// radixSortFloat64 performs an unsigned radix sort on float64 data and returns a
// slice of indices that represent the order of the sorted data. The IEEE-754
// representation is transformed so that the lexicographic order of the keys
// matches the numeric order of the floats.
func radixSortFloat64(data []float64, ascending bool) []int {
	n := len(data)
	if n == 0 {
		return nil
	}

	keys := make([]uint64, n)
	for i, v := range data {
		bits := math.Float64bits(v)
		// For positive floats, flip the sign bit; for negative floats, invert all
		// bits so that the ordering of the resulting unsigned integers matches
		// the ordering of the original floats.
		if bits>>63 == 0 {
			keys[i] = bits ^ 0x8000000000000000
		} else {
			keys[i] = ^bits
		}
	}

	return radixSortUint64Keys(keys, ascending)
}

// radixSortUint64Keys is the work-horse that performs a radix sort on the given
// uint64 keys and returns the ordering of the original positions. It uses an
// 8-bit base (256 buckets) and runs eight passes to cover all 64 bits.
func radixSortUint64Keys(keys []uint64, ascending bool) []int {
	const (
		bitsPerPass = 8
		buckets     = 1 << bitsPerPass // 256
		passes      = 64 / bitsPerPass // 8
	)

	n := len(keys)
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	tmp := make([]int, n)
	counts := make([]int, buckets)

	for pass := 0; pass < passes; pass++ {
		// Clear bucket counters.
		for i := range counts {
			counts[i] = 0
		}

		shift := uint(pass * bitsPerPass)

		// Histogram collection.
		for _, idx := range indices {
			b := int((keys[idx] >> shift) & 0xFF)
			counts[b]++
		}

		// Prefix scan to convert counts -> starting offsets.
		if ascending {
			sum := 0
			for i := 0; i < buckets; i++ {
				c := counts[i]
				counts[i] = sum
				sum += c
			}
		} else {
			sum := 0
			for i := buckets - 1; i >= 0; i-- {
				c := counts[i]
				counts[i] = sum
				sum += c
			}
		}

		// Scatter into temporary slice based on bucket offsets.
		for _, idx := range indices {
			b := int((keys[idx] >> shift) & 0xFF)
			pos := counts[b]
			tmp[pos] = idx
			counts[b]++
		}

		// Prepare for next pass.
		copy(indices, tmp)
	}

	return indices
}
