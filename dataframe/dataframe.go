package dataframe

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sort"
	"sync"

	"go-polars/types"

	xxhash "github.com/cespare/xxhash/v2"
)

// DataFrame represents a collection of Series with the same length
type DataFrame struct {
	series map[string]*types.Series
	length int
}

// New creates a new DataFrame from a map of Series
func New(series map[string]*types.Series) (*DataFrame, error) {
	if len(series) == 0 {
		return &DataFrame{
			series: make(map[string]*types.Series),
			length: 0,
		}, nil
	}

	// Get length from first series
	var length int
	for _, s := range series {
		length = s.Length
		break
	}

	// Verify all series have the same length
	for name, s := range series {
		if s.Length != length {
			return nil, fmt.Errorf("series %s has length %d, expected %d", name, s.Length, length)
		}
	}

	return &DataFrame{
		series: series,
		length: length,
	}, nil
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(columns []string) (*DataFrame, error) {
	selected := make(map[string]*types.Series)
	for _, col := range columns {
		series, ok := df.series[col]
		if !ok {
			return nil, fmt.Errorf("column %s not found", col)
		}
		selected[col] = series
	}
	return New(selected)
}

// Filter returns a new DataFrame with only the rows that satisfy the predicate
func (df *DataFrame) Filter(column string, predicate func(interface{}) bool) (*DataFrame, error) {
	series, ok := df.series[column]
	if !ok {
		return nil, fmt.Errorf("column %s not found", column)
	}

	// Create mask of rows to keep
	mask := make([]bool, df.length)
	switch data := series.Data.(type) {
	case []int64:
		for i, val := range data {
			mask[i] = predicate(val)
		}
	case []float64:
		for i, val := range data {
			mask[i] = predicate(val)
		}
	case []string:
		for i, val := range data {
			mask[i] = predicate(val)
		}
	case []bool:
		for i, val := range data {
			mask[i] = predicate(val)
		}
	default:
		return nil, fmt.Errorf("unsupported data type for column %s", column)
	}

	// Apply mask to all series
	filtered := make(map[string]*types.Series)
	for name, s := range df.series {
		switch data := s.Data.(type) {
		case []int64:
			newData := make([]int64, 0)
			for i, keep := range mask {
				if keep {
					newData = append(newData, data[i])
				}
			}
			filtered[name] = types.NewSeries(name, newData)
		case []float64:
			newData := make([]float64, 0)
			for i, keep := range mask {
				if keep {
					newData = append(newData, data[i])
				}
			}
			filtered[name] = types.NewSeries(name, newData)
		case []string:
			newData := make([]string, 0)
			for i, keep := range mask {
				if keep {
					newData = append(newData, data[i])
				}
			}
			filtered[name] = types.NewSeries(name, newData)
		case []bool:
			newData := make([]bool, 0)
			for i, keep := range mask {
				if keep {
					newData = append(newData, data[i])
				}
			}
			filtered[name] = types.NewSeries(name, newData)
		}
	}

	return New(filtered)
}

// Shape returns the dimensions of the DataFrame (rows, columns)
func (df *DataFrame) Shape() (int, int) {
	return df.length, len(df.series)
}

// Columns returns the column names of the DataFrame
func (df *DataFrame) Columns() []string {
	cols := make([]string, 0, len(df.series))
	for name := range df.series {
		cols = append(cols, name)
	}
	return cols
}

// Head returns a new DataFrame with the first n rows
func (df *DataFrame) Head(n int) (*DataFrame, error) {
	if n > df.length {
		n = df.length
	}

	head := make(map[string]*types.Series)
	for name, s := range df.series {
		switch data := s.Data.(type) {
		case []int64:
			head[name] = types.NewSeries(name, data[:n])
		case []float64:
			head[name] = types.NewSeries(name, data[:n])
		case []string:
			head[name] = types.NewSeries(name, data[:n])
		case []bool:
			head[name] = types.NewSeries(name, data[:n])
		}
	}

	return New(head)
}

// SortByColumn sorts the DataFrame by the specified column
func (df *DataFrame) SortByColumn(column string, ascending bool) (*DataFrame, error) {
	series, ok := df.series[column]
	if !ok {
		return nil, fmt.Errorf("column %s not found", column)
	}

	// Create index slice to track original positions
	indices := make([]int, df.length)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on the column values
	switch data := series.Data.(type) {
	case []int64:
		keys := make([]uint64, len(data))
		for i, v := range data {
			keys[i] = uint64(v) ^ 0x8000000000000000
		}
		indices = ParallelRadixSortUint64(keys, ascending)
	case []float64:
		keys := make([]uint64, len(data))
		for i, v := range data {
			bits := math.Float64bits(v)
			if bits>>63 == 0 {
				keys[i] = bits ^ 0x8000000000000000
			} else {
				keys[i] = ^bits
			}
		}
		indices = ParallelRadixSortUint64(keys, ascending)
	case []string:
		sort.Slice(indices, func(i, j int) bool {
			if ascending {
				return data[indices[i]] < data[indices[j]]
			}
			return data[indices[i]] > data[indices[j]]
		})
	case []bool:
		sort.Slice(indices, func(i, j int) bool {
			if ascending {
				return !data[indices[i]] && data[indices[j]]
			}
			return data[indices[i]] && !data[indices[j]]
		})
	default:
		return nil, fmt.Errorf("unsupported data type for column %s", column)
	}

	// Permute each column in place using indices
	for _, s := range df.series {
		switch data := s.Data.(type) {
		case []int64:
			inPlacePermuteInt64(data, indices)
		case []float64:
			inPlacePermuteFloat64(data, indices)
		case []string:
			inPlacePermuteString(data, indices)
		case []bool:
			inPlacePermuteBool(data, indices)
		}
	}

	// Return the same DataFrame (mutated) to keep API unchanged.
	return df, nil
}

// SortByIndex sorts the DataFrame by the row index
func (df *DataFrame) SortByIndex(ascending bool) (*DataFrame, error) {
	// Create index slice
	indices := make([]int, df.length)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices
	if ascending {
		sort.Ints(indices)
	} else {
		sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	}

	// Create new sorted series
	sorted := make(map[string]*types.Series)
	for name, s := range df.series {
		switch data := s.Data.(type) {
		case []int64:
			newData := make([]int64, df.length)
			for newIdx, oldIdx := range indices {
				newData[newIdx] = data[oldIdx]
			}
			sorted[name] = types.NewSeries(name, newData)
		case []float64:
			newData := make([]float64, df.length)
			for newIdx, oldIdx := range indices {
				newData[newIdx] = data[oldIdx]
			}
			sorted[name] = types.NewSeries(name, newData)
		case []string:
			newData := make([]string, df.length)
			for newIdx, oldIdx := range indices {
				newData[newIdx] = data[oldIdx]
			}
			sorted[name] = types.NewSeries(name, newData)
		case []bool:
			newData := make([]bool, df.length)
			for newIdx, oldIdx := range indices {
				newData[newIdx] = data[oldIdx]
			}
			sorted[name] = types.NewSeries(name, newData)
		}
	}

	return New(sorted)
}

// AggregationType represents the type of aggregation to perform
type AggregationType int

const (
	Sum AggregationType = iota
	Mean
	Count
	Min
	Max
)

// GroupBy groups the DataFrame by one or more columns
func (df *DataFrame) GroupBy(columns []string) (*GroupedDataFrame, error) {
	// Verify columns exist
	for _, col := range columns {
		if _, ok := df.series[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		}
	}

	// Defer actual grouping work until Aggregate to enable a single-pass
	// streaming aggregation. This minimises memory usage by avoiding the
	// per-group []int slice that previously stored row indices.
	return &GroupedDataFrame{
		df:      df,
		groups:  nil, // will be filled lazily if needed
		columns: columns,
	}, nil
}

// key128 is a simple 128-bit hash key used for grouping. It is comparable, so
// it can be used directly as a map key without additional allocations.
type key128 struct {
	hi uint64
	lo uint64
}

// GroupedDataFrame represents a grouped DataFrame
type GroupedDataFrame struct {
	df      *DataFrame
	groups  map[key128][]int
	columns []string
}

// Aggregate performs the specified aggregation on the grouped DataFrame
func (gdf *GroupedDataFrame) Aggregate(column string, aggType AggregationType) (*DataFrame, error) {
	series, ok := gdf.df.series[column]
	if !ok {
		return nil, fmt.Errorf("column %s not found", column)
	}

	// Fast streaming path: if groups map is nil or empty, build aggregation in
	// a single pass without allocating per-group index slices.
	if gdf.groups == nil || len(gdf.groups) == 0 {
		return gdf.aggregateStreaming(column, series, aggType)
	}

	// === Legacy path (uses precomputed []int indices) ======================

	// Create result series for group columns and aggregated column
	resultSeries := make(map[string]*types.Series)
	length := len(gdf.groups)

	// Initialize series for group columns
	for _, col := range gdf.columns {
		switch gdf.df.series[col].Data.(type) {
		case []int64:
			resultSeries[col] = types.NewSeries(col, make([]int64, length))
		case []float64:
			resultSeries[col] = types.NewSeries(col, make([]float64, length))
		case []string:
			resultSeries[col] = types.NewSeries(col, make([]string, length))
		case []bool:
			resultSeries[col] = types.NewSeries(col, make([]bool, length))
		}
	}

	// Initialize aggregated column
	var aggData interface{}
	switch series.Data.(type) {
	case []int64:
		aggData = make([]int64, length)
	case []float64:
		aggData = make([]float64, length)
	default:
		return nil, fmt.Errorf("unsupported data type for aggregation")
	}
	resultSeries[column] = types.NewSeries(column, aggData)

	// Perform aggregation for each group
	i := 0
	for _, indices := range gdf.groups {
		// Set group column values
		for _, col := range gdf.columns {
			series := gdf.df.series[col]
			switch data := series.Data.(type) {
			case []int64:
				resultSeries[col].Data.([]int64)[i] = data[indices[0]]
			case []float64:
				resultSeries[col].Data.([]float64)[i] = data[indices[0]]
			case []string:
				resultSeries[col].Data.([]string)[i] = data[indices[0]]
			case []bool:
				resultSeries[col].Data.([]bool)[i] = data[indices[0]]
			}
		}

		// Perform aggregation
		switch data := series.Data.(type) {
		case []int64:
			var result int64
			switch aggType {
			case Sum:
				result = sumInt64Indexed(data, indices)
			case Mean:
				result = sumInt64Indexed(data, indices) / int64(len(indices))
			case Count:
				result = int64(len(indices))
			case Min:
				result = minInt64Indexed(data, indices)
			case Max:
				result = maxInt64Indexed(data, indices)
			}
			resultSeries[column].Data.([]int64)[i] = result
		case []float64:
			var result float64
			switch aggType {
			case Sum:
				result = sumFloat64Indexed(data, indices)
			case Mean:
				result = sumFloat64Indexed(data, indices) / float64(len(indices))
			case Count:
				result = float64(len(indices))
			case Min:
				result = minFloat64Indexed(data, indices)
			case Max:
				result = maxFloat64Indexed(data, indices)
			}
			resultSeries[column].Data.([]float64)[i] = result
		}
		i++
	}

	return New(resultSeries)
}

// aggregateStreaming performs a single-pass aggregation without allocating
// per-group index slices. It is called when GroupBy deferred building the map.
func (gdf *GroupedDataFrame) aggregateStreaming(column string, series *types.Series, aggType AggregationType) (*DataFrame, error) {
	if gdf.groups == nil {
		gdf.groups = make(map[key128][]int)
	}
	// Define aggregation state containers.
	type int64State struct {
		sum   int64
		min   int64
		max   int64
		count int64
		rep   int // representative row index for group column extraction
	}
	type float64State struct {
		sum   float64
		min   float64
		max   float64
		count int64
		rep   int
	}

	// Containers per key.
	intStates := make(map[key128]*int64State)
	floatStates := make(map[key128]*float64State)

	// Convenience for value series data switch.
	switch data := series.Data.(type) {
	case []int64:
		rows := len(data)
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}

		// Use parallel path for larger datasets (> 50k) and multiple CPUs.
		if rows >= 50000 && workers > 1 {
			shard := (rows + workers - 1) / workers
			local := make([]map[key128]*int64State, workers)
			var wg sync.WaitGroup
			wg.Add(workers)

			for w := 0; w < workers; w++ {
				start := w * shard
				end := start + shard
				if end > rows {
					end = rows
				}
				go func(slot, s, e int) {
					defer wg.Done()
					m := make(map[key128]*int64State)
					for i := s; i < e; i++ {
						v := data[i]
						var hi, lo uint64
						for colIdx, col := range gdf.columns {
							s := gdf.df.series[col]
							var hv uint64
							switch colData := s.Data.(type) {
							case []int64:
								var buf [8]byte
								binary.LittleEndian.PutUint64(buf[:], uint64(colData[i]))
								hv = xxhash.Sum64(buf[:])
							case []float64:
								var buf [8]byte
								binary.LittleEndian.PutUint64(buf[:], math.Float64bits(colData[i]))
								hv = xxhash.Sum64(buf[:])
							case []string:
								hv = xxhash.Sum64String(colData[i])
							case []bool:
								var buf [8]byte
								var b uint64
								if colData[i] {
									b = 1
								}
								binary.LittleEndian.PutUint64(buf[:], b)
								hv = xxhash.Sum64(buf[:])
							}

							shift := uint(colIdx*11) & 63
							if colIdx%2 == 0 {
								hi ^= bits.RotateLeft64(hv, int(shift))
							} else {
								lo ^= bits.RotateLeft64(hv, int(shift))
							}
						}

						k := key128{hi: hi, lo: lo}
						st, ok := m[k]
						if !ok {
							st = &int64State{min: v, max: v, rep: i}
							m[k] = st
						}
						if aggType == Sum || aggType == Mean {
							st.sum += v
						}
						if v < st.min {
							st.min = v
						}
						if v > st.max {
							st.max = v
						}
						st.count++
					}
					local[slot] = m
				}(w, start, end)
			}
			wg.Wait()

			// Merge local maps into intStates
			for _, m := range local {
				for k, st := range m {
					dst, ok := intStates[k]
					if !ok {
						intStates[k] = st
						continue
					}
					dst.sum += st.sum
					dst.count += st.count
					if st.min < dst.min {
						dst.min = st.min
					}
					if st.max > dst.max {
						dst.max = st.max
					}
				}
			}

		} else {
			// Single-threaded fall-back (previous logic)
			for i, v := range data {
				var hi, lo uint64
				for colIdx, col := range gdf.columns {
					s := gdf.df.series[col]
					var hv uint64
					switch colData := s.Data.(type) {
					case []int64:
						var buf [8]byte
						binary.LittleEndian.PutUint64(buf[:], uint64(colData[i]))
						hv = xxhash.Sum64(buf[:])
					case []float64:
						var buf [8]byte
						binary.LittleEndian.PutUint64(buf[:], math.Float64bits(colData[i]))
						hv = xxhash.Sum64(buf[:])
					case []string:
						hv = xxhash.Sum64String(colData[i])
					case []bool:
						var buf [8]byte
						var b uint64
						if colData[i] {
							b = 1
						}
						binary.LittleEndian.PutUint64(buf[:], b)
						hv = xxhash.Sum64(buf[:])
					}

					shift := uint(colIdx*11) & 63
					if colIdx%2 == 0 {
						hi ^= bits.RotateLeft64(hv, int(shift))
					} else {
						lo ^= bits.RotateLeft64(hv, int(shift))
					}
				}

				k := key128{hi: hi, lo: lo}
				st, ok := intStates[k]
				if !ok {
					st = &int64State{min: v, max: v, rep: i}
					intStates[k] = st
				}

				if aggType == Sum || aggType == Mean {
					st.sum += v
				}
				if v < st.min {
					st.min = v
				}
				if v > st.max {
					st.max = v
				}
				st.count++
			}
		}

		// Build result series.
		length := len(intStates)
		resultSeries := make(map[string]*types.Series)

		// Init group column series
		for _, col := range gdf.columns {
			switch gdf.df.series[col].Data.(type) {
			case []int64:
				resultSeries[col] = types.NewSeries(col, make([]int64, length))
			case []float64:
				resultSeries[col] = types.NewSeries(col, make([]float64, length))
			case []string:
				resultSeries[col] = types.NewSeries(col, make([]string, length))
			case []bool:
				resultSeries[col] = types.NewSeries(col, make([]bool, length))
			}
		}

		aggData := make([]int64, length)
		resultSeries[column] = types.NewSeries(column, aggData)

		idx := 0
		for k, st := range intStates {
			// Set group column values from representative row
			rep := st.rep
			for _, col := range gdf.columns {
				s := gdf.df.series[col]
				switch colData := s.Data.(type) {
				case []int64:
					resultSeries[col].Data.([]int64)[idx] = colData[rep]
				case []float64:
					resultSeries[col].Data.([]float64)[idx] = colData[rep]
				case []string:
					resultSeries[col].Data.([]string)[idx] = colData[rep]
				case []bool:
					resultSeries[col].Data.([]bool)[idx] = colData[rep]
				}
			}

			// Finalise aggregation value
			var out int64
			switch aggType {
			case Sum:
				out = st.sum
			case Mean:
				out = st.sum / st.count
			case Count:
				out = st.count
			case Min:
				out = st.min
			case Max:
				out = st.max
			}
			aggData[idx] = out

			// Optionally store back into groups map for later reuse.
			gdf.groups[k] = []int{rep} // minimal placeholder
			idx++
		}

		return New(resultSeries)

	case []float64:
		rows := len(data)
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}

		if rows >= 50000 && workers > 1 {
			shard := (rows + workers - 1) / workers
			local := make([]map[key128]*float64State, workers)
			var wg sync.WaitGroup
			wg.Add(workers)

			for w := 0; w < workers; w++ {
				start := w * shard
				end := start + shard
				if end > rows {
					end = rows
				}
				go func(slot, s, e int) {
					defer wg.Done()
					m := make(map[key128]*float64State)
					for i := s; i < e; i++ {
						v := data[i]
						var hi, lo uint64
						for colIdx, col := range gdf.columns {
							s := gdf.df.series[col]
							var hv uint64
							switch colData := s.Data.(type) {
							case []int64:
								var buf [8]byte
								binary.LittleEndian.PutUint64(buf[:], uint64(colData[i]))
								hv = xxhash.Sum64(buf[:])
							case []float64:
								var buf [8]byte
								binary.LittleEndian.PutUint64(buf[:], math.Float64bits(colData[i]))
								hv = xxhash.Sum64(buf[:])
							case []string:
								hv = xxhash.Sum64String(colData[i])
							case []bool:
								var buf [8]byte
								var b uint64
								if colData[i] {
									b = 1
								}
								binary.LittleEndian.PutUint64(buf[:], b)
								hv = xxhash.Sum64(buf[:])
							}

							shift := uint(colIdx*11) & 63
							if colIdx%2 == 0 {
								hi ^= bits.RotateLeft64(hv, int(shift))
							} else {
								lo ^= bits.RotateLeft64(hv, int(shift))
							}
						}

						k := key128{hi: hi, lo: lo}
						st, ok := m[k]
						if !ok {
							st = &float64State{min: v, max: v, rep: i}
							m[k] = st
						}
						if aggType == Sum || aggType == Mean {
							st.sum += v
						}
						if v < st.min {
							st.min = v
						}
						if v > st.max {
							st.max = v
						}
						st.count++
					}
					local[slot] = m
				}(w, start, end)
			}

			wg.Wait()

			// Merge local maps
			for _, m := range local {
				for k, st := range m {
					dst, ok := floatStates[k]
					if !ok {
						floatStates[k] = st
						continue
					}
					dst.sum += st.sum
					dst.count += st.count
					if st.min < dst.min {
						dst.min = st.min
					}
					if st.max > dst.max {
						dst.max = st.max
					}
				}
			}

		} else {
			for i, v := range data {
				var hi, lo uint64
				for colIdx, col := range gdf.columns {
					s := gdf.df.series[col]
					var hv uint64
					switch colData := s.Data.(type) {
					case []int64:
						var buf [8]byte
						binary.LittleEndian.PutUint64(buf[:], uint64(colData[i]))
						hv = xxhash.Sum64(buf[:])
					case []float64:
						var buf [8]byte
						binary.LittleEndian.PutUint64(buf[:], math.Float64bits(colData[i]))
						hv = xxhash.Sum64(buf[:])
					case []string:
						hv = xxhash.Sum64String(colData[i])
					case []bool:
						var buf [8]byte
						var b uint64
						if colData[i] {
							b = 1
						}
						binary.LittleEndian.PutUint64(buf[:], b)
						hv = xxhash.Sum64(buf[:])
					}

					shift := uint(colIdx*11) & 63
					if colIdx%2 == 0 {
						hi ^= bits.RotateLeft64(hv, int(shift))
					} else {
						lo ^= bits.RotateLeft64(hv, int(shift))
					}
				}

				k := key128{hi: hi, lo: lo}
				st, ok := floatStates[k]
				if !ok {
					st = &float64State{min: v, max: v, rep: i}
					floatStates[k] = st
				}

				if aggType == Sum || aggType == Mean {
					st.sum += v
				}
				if v < st.min {
					st.min = v
				}
				if v > st.max {
					st.max = v
				}
				st.count++
			}
		}

		// Build result series as before
		length := len(floatStates)
		resultSeries := make(map[string]*types.Series)
		for _, col := range gdf.columns {
			switch gdf.df.series[col].Data.(type) {
			case []int64:
				resultSeries[col] = types.NewSeries(col, make([]int64, length))
			case []float64:
				resultSeries[col] = types.NewSeries(col, make([]float64, length))
			case []string:
				resultSeries[col] = types.NewSeries(col, make([]string, length))
			case []bool:
				resultSeries[col] = types.NewSeries(col, make([]bool, length))
			}
		}

		aggData := make([]float64, length)
		resultSeries[column] = types.NewSeries(column, aggData)

		idx := 0
		for k, st := range floatStates {
			rep := st.rep
			for _, col := range gdf.columns {
				s := gdf.df.series[col]
				switch colData := s.Data.(type) {
				case []int64:
					resultSeries[col].Data.([]int64)[idx] = colData[rep]
				case []float64:
					resultSeries[col].Data.([]float64)[idx] = colData[rep]
				case []string:
					resultSeries[col].Data.([]string)[idx] = colData[rep]
				case []bool:
					resultSeries[col].Data.([]bool)[idx] = colData[rep]
				}
			}

			var out float64
			switch aggType {
			case Sum:
				out = st.sum
			case Mean:
				out = st.sum / float64(st.count)
			case Count:
				out = float64(st.count)
			case Min:
				out = st.min
			case Max:
				out = st.max
			}
			aggData[idx] = out

			gdf.groups[k] = []int{rep}
			idx++
		}

		return New(resultSeries)
	default:
		return nil, fmt.Errorf("unsupported data type for aggregation")
	}
}
