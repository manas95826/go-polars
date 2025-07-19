package dataframe

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	xxhash "github.com/cespare/xxhash/v2"
)

// buildKey128 constructs a deterministic 128-bit hash key for the given row
// using the supplied grouping columns. The algorithm mirrors the one used in
// aggregateStreaming so that both the streaming and sort-based paths can share
// the same key space.
func buildKey128(df *DataFrame, columns []string, row int) key128 {
	var hi, lo uint64

	for colIdx, col := range columns {
		s := df.series[col]
		var hv uint64

		switch colData := s.Data.(type) {
		case []int64:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(colData[row]))
			hv = xxhash.Sum64(buf[:])
		case []float64:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(colData[row]))
			hv = xxhash.Sum64(buf[:])
		case []string:
			hv = xxhash.Sum64String(colData[row])
		case []bool:
			var buf [8]byte
			var b uint64
			if colData[row] {
				b = 1
			}
			binary.LittleEndian.PutUint64(buf[:], b)
			hv = xxhash.Sum64(buf[:])
		default:
			// Unsupported types fall back to zero hash – this still provides
			// determinism but may lead to collisions for exotic column types.
			hv = 0
		}

		shift := uint(colIdx*11) & 63
		if colIdx%2 == 0 {
			hi ^= bits.RotateLeft64(hv, int(shift))
		} else {
			lo ^= bits.RotateLeft64(hv, int(shift))
		}
	}

	return key128{hi: hi, lo: lo}
}

// sortAggregateInt64 is the planned sort-based aggregation path for int64
// value columns. It is currently a stub – functionality will be implemented in
// a follow-up patch.
func sortAggregateInt64(df *DataFrame, columns []string, values []int64, aggType AggregationType) (*DataFrame, error) {
	_ = df
	_ = columns
	_ = values
	_ = aggType
	return nil, errors.New("sortAggregateInt64: not implemented")
}

// sortAggregateFloat64 is the planned sort-based aggregation path for float64
// value columns. It is currently a stub – functionality will be implemented in
// a follow-up patch.
func sortAggregateFloat64(df *DataFrame, columns []string, values []float64, aggType AggregationType) (*DataFrame, error) {
	_ = df
	_ = columns
	_ = values
	_ = aggType
	return nil, errors.New("sortAggregateFloat64: not implemented")
}
