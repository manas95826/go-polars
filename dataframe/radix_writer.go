//go:build !purego
// +build !purego

package dataframe

import (
	"runtime"
	"sync"
)

// radixSortUint64KeysParallel performs an in-place, stable LSD radix sort on keys and
// returns the ordering indices. It parallelises each pass across shards to avoid
// a separate merge step.
func radixSortUint64KeysParallel(keys []uint64, ascending bool) []int {
	n := len(keys)
	if n <= 1 {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		return idx
	}

	const (
		buckets     = 256
		bitsPerPass = 8
		passes      = 64 / bitsPerPass
	)

	workers := runtime.GOMAXPROCS(0)
	if workers < 2 || n < 1<<15 {
		return radixSortUint64Keys(keys, ascending)
	}

	// Initial index slice 0..n-1
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	tmp := make([]int, n)

	shardSize := (n + workers - 1) / workers

	// counts[w][bucket]
	counts := make([][]int, workers)
	for w := 0; w < workers; w++ {
		counts[w] = make([]int, buckets)
	}

	preOffsets := make([][]int, workers)
	for w := 0; w < workers; w++ {
		preOffsets[w] = make([]int, buckets)
	}

	var wg sync.WaitGroup

	for pass := 0; pass < passes; pass++ {
		shift := uint(pass * bitsPerPass)

		// Zero counts
		for w := 0; w < workers; w++ {
			for b := 0; b < buckets; b++ {
				counts[w][b] = 0
			}
		}

		// Histogram per shard
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			start := w * shardSize
			end := start + shardSize
			if end > n {
				end = n
			}
			if start >= end {
				wg.Done()
				continue
			}
			go func(slot, s, e int) {
				localCounts := counts[slot]
				for i := s; i < e; i++ {
					b := (keys[indices[i]] >> shift) & 0xFF
					localCounts[b]++
				}
				wg.Done()
			}(w, start, end)
		}
		wg.Wait()

		// Global offsets
		global := make([]int, buckets)
		for b := 0; b < buckets; b++ {
			sum := 0
			for w := 0; w < workers; w++ {
				sum += counts[w][b]
			}
			global[b] = sum
		}
		// prefix-sum over global to get bucket start
		running := 0
		for b := 0; b < buckets; b++ {
			tmpVal := global[b]
			global[b] = running
			running += tmpVal
		}

		// Compute per-shard prefix (preOffsets)
		for b := 0; b < buckets; b++ {
			offset := global[b]
			for w := 0; w < workers; w++ {
				preOffsets[w][b] = offset
				offset += counts[w][b]
			}
		}

		// Scatter into tmp based on offsets
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			start := w * shardSize
			end := start + shardSize
			if end > n {
				end = n
			}
			if start >= end {
				wg.Done()
				continue
			}
			go func(slot, s, e int) {
				localOffsets := preOffsets[slot]
				for i := s; i < e; i++ {
					idx := indices[i]
					b := (keys[idx] >> shift) & 0xFF
					pos := localOffsets[b]
					tmp[pos] = idx
					localOffsets[b]++
				}
				wg.Done()
			}(w, start, end)
		}
		wg.Wait()

		// Swap slices for next pass
		indices, tmp = tmp, indices
	}

	if !ascending {
		// reverse indices
		for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
			indices[i], indices[j] = indices[j], indices[i]
		}
	}

	return indices
}
