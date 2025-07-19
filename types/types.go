package types

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DataType represents the type of data in a Series
type DataType interface {
	String() string
}

// Primitive data types
type (
	Int64Type   struct{}
	Float64Type struct{}
	StringType  struct{}
	BooleanType struct{}
)

func (Int64Type) String() string   { return "Int64" }
func (Float64Type) String() string { return "Float64" }
func (StringType) String() string  { return "String" }
func (BooleanType) String() string { return "Boolean" }

// Series represents a single column of data
type Series struct {
	Name     string
	DataType DataType
	Data     interface{} // Will hold []int64, []float64, []string, or []bool
	Length   int
}

// NewSeries creates a new Series with the given name and data
func NewSeries(name string, data interface{}) *Series {
	switch d := data.(type) {
	case []int64:
		return &Series{
			Name:     name,
			DataType: Int64Type{},
			Data:     d,
			Length:   len(d),
		}
	case []float64:
		return &Series{
			Name:     name,
			DataType: Float64Type{},
			Data:     d,
			Length:   len(d),
		}
	case []string:
		return &Series{
			Name:     name,
			DataType: StringType{},
			Data:     d,
			Length:   len(d),
		}
	case []bool:
		return &Series{
			Name:     name,
			DataType: BooleanType{},
			Data:     d,
			Length:   len(d),
		}
	default:
		panic("unsupported data type")
	}
}

// DataFrame represents a collection of Series with the same length
type DataFrame struct {
	Series       map[string]*Series
	Length       int
	GroupIndices map[string][]int
	GroupColumns []string
}

// New creates a new DataFrame from a map of Series
func New(series map[string]*Series) (*DataFrame, error) {
	if len(series) == 0 {
		return &DataFrame{
			Series:       make(map[string]*Series),
			Length:       0,
			GroupIndices: nil,
			GroupColumns: nil,
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
		Series:       series,
		Length:       length,
		GroupIndices: nil,
		GroupColumns: nil,
	}, nil
}

// Shape returns the dimensions of the DataFrame (rows, columns)
func (df *DataFrame) Shape() (int, int) {
	if df == nil || df.Series == nil {
		return 0, 0
	}
	return df.Length, len(df.Series)
}

// Columns returns the column names of the DataFrame
func (df *DataFrame) Columns() []string {
	if df == nil || df.Series == nil {
		return []string{}
	}
	cols := make([]string, 0, len(df.Series))
	for name := range df.Series {
		cols = append(cols, name)
	}
	return cols
}

// Head returns a new DataFrame with the first n rows
func (df *DataFrame) Head(n int) (*DataFrame, error) {
	if df == nil || df.Series == nil {
		return nil, fmt.Errorf("DataFrame is nil or empty")
	}
	if n > df.Length {
		n = df.Length
	}

	head := make(map[string]*Series)
	for name, s := range df.Series {
		switch data := s.Data.(type) {
		case []int64:
			head[name] = NewSeries(name, data[:n])
		case []float64:
			head[name] = NewSeries(name, data[:n])
		case []string:
			head[name] = NewSeries(name, data[:n])
		case []bool:
			head[name] = NewSeries(name, data[:n])
		}
	}

	return New(head)
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
func (df *DataFrame) GroupBy(columns []string) (*DataFrame, error) {
	if df == nil || df.Series == nil {
		return nil, fmt.Errorf("DataFrame is nil or empty")
	}

	// Verify all columns exist
	for _, col := range columns {
		if _, ok := df.Series[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		}
	}

	// === Fast path: single-column groupby ==================================
	if len(columns) == 1 {
		col := columns[0]
		s := df.Series[col]

		switch data := s.Data.(type) {
		case []int64:
			groups := make(map[int64][]int, df.Length)
			for i, v := range data {
				groups[v] = append(groups[v], i)
			}
			return buildGroupedDataFrameSingleInt64(df, col, groups)
		case []float64:
			groups := make(map[float64][]int, df.Length)
			for i, v := range data {
				groups[v] = append(groups[v], i)
			}
			return buildGroupedDataFrameSingleFloat64(df, col, groups)
		case []string:
			groups := make(map[string][]int, df.Length)
			for i, v := range data {
				groups[v] = append(groups[v], i)
			}
			return buildGroupedDataFrameSingleString(df, col, groups)
		case []bool:
			groups := make(map[bool][]int, 2)
			for i, v := range data {
				groups[v] = append(groups[v], i)
			}
			return buildGroupedDataFrameSingleBool(df, col, groups)
		default:
			// Fallback to generic implementation below
		}
	}

	// === Generic (multi-column) implementation =============================

	// Create a map of group keys to row indices
	groups := make(map[string][]int)
	var builder strings.Builder

	for i := 0; i < df.Length; i++ {
		builder.Reset()
		for _, col := range columns {
			series := df.Series[col]
			switch data := series.Data.(type) {
			case []int64:
				builder.WriteString(strconv.FormatInt(data[i], 10))
			case []float64:
				builder.WriteString(strconv.FormatFloat(data[i], 'f', -1, 64))
			case []string:
				builder.WriteString(data[i])
			case []bool:
				if data[i] {
					builder.WriteByte('1')
				} else {
					builder.WriteByte('0')
				}
			}
			builder.WriteByte('_')
		}
		key := builder.String()
		groups[key] = append(groups[key], i)
	}

	return df.buildGroupedDataFrameMulti(columns, groups)
}

// Helper functions specialised per key type to avoid generics (which are not
// yet allowed on methods).

func buildGroupedDataFrameSingleInt64(df *DataFrame, column string, groups map[int64][]int) (*DataFrame, error) {
	// Gather keys deterministically
	keys := make([]int64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// Build result series
	resultSeries := map[string]*Series{
		column: NewSeries(column, keys),
	}

	groupIndices := make(map[string][]int, len(groups))
	for _, k := range keys {
		groupIndices[strconv.FormatInt(k, 10)] = groups[k]
	}

	// Reference other columns
	for name, s := range df.Series {
		if name != column {
			resultSeries[name] = s
		}
	}

	return &DataFrame{Series: resultSeries, Length: len(keys), GroupIndices: groupIndices, GroupColumns: []string{column}}, nil
}

func buildGroupedDataFrameSingleFloat64(df *DataFrame, column string, groups map[float64][]int) (*DataFrame, error) {
	keys := make([]float64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Float64s(keys)

	resultSeries := map[string]*Series{
		column: NewSeries(column, keys),
	}

	groupIndices := make(map[string][]int, len(groups))
	for _, k := range keys {
		groupIndices[strconv.FormatFloat(k, 'f', -1, 64)] = groups[k]
	}

	for name, s := range df.Series {
		if name != column {
			resultSeries[name] = s
		}
	}

	return &DataFrame{Series: resultSeries, Length: len(keys), GroupIndices: groupIndices, GroupColumns: []string{column}}, nil
}

func buildGroupedDataFrameSingleString(df *DataFrame, column string, groups map[string][]int) (*DataFrame, error) {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	resultSeries := map[string]*Series{
		column: NewSeries(column, keys),
	}

	groupIndices := make(map[string][]int, len(groups))
	for _, k := range keys {
		groupIndices[k] = groups[k]
	}

	for name, s := range df.Series {
		if name != column {
			resultSeries[name] = s
		}
	}

	return &DataFrame{Series: resultSeries, Length: len(keys), GroupIndices: groupIndices, GroupColumns: []string{column}}, nil
}

func buildGroupedDataFrameSingleBool(df *DataFrame, column string, groups map[bool][]int) (*DataFrame, error) {
	// keys order: false, true
	keys := []bool{false, true}
	// Filter to existing keys
	uniqueKeys := make([]bool, 0, 2)
	for _, k := range keys {
		if _, ok := groups[k]; ok {
			uniqueKeys = append(uniqueKeys, k)
		}
	}

	resultSeries := map[string]*Series{
		column: NewSeries(column, uniqueKeys),
	}

	groupIndices := make(map[string][]int, len(uniqueKeys))
	for _, k := range uniqueKeys {
		if k {
			groupIndices["true"] = groups[k]
		} else {
			groupIndices["false"] = groups[k]
		}
	}

	for name, s := range df.Series {
		if name != column {
			resultSeries[name] = s
		}
	}

	return &DataFrame{Series: resultSeries, Length: len(uniqueKeys), GroupIndices: groupIndices, GroupColumns: []string{column}}, nil
}

// buildGroupedDataFrameMulti handles the generic (multi-column) grouping path.
func (df *DataFrame) buildGroupedDataFrameMulti(columns []string, groups map[string][]int) (*DataFrame, error) {
	// Create result series for group columns
	resultSeries := make(map[string]*Series)
	length := len(groups)

	// Initialize series for group columns
	for _, col := range columns {
		switch df.Series[col].Data.(type) {
		case []int64:
			resultSeries[col] = NewSeries(col, make([]int64, length))
		case []float64:
			resultSeries[col] = NewSeries(col, make([]float64, length))
		case []string:
			resultSeries[col] = NewSeries(col, make([]string, length))
		case []bool:
			resultSeries[col] = NewSeries(col, make([]bool, length))
		}
	}

	// Set group column values
	i := 0
	for _, indices := range groups {
		if len(indices) == 0 {
			continue
		}
		for _, col := range columns {
			series := df.Series[col]
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
		i++
	}

	// Create new DataFrame with group columns and indices
	result := &DataFrame{
		Series:       resultSeries,
		Length:       i,
		GroupIndices: groups,
		GroupColumns: columns,
	}

	// Copy all series from original DataFrame
	for name, s := range df.Series {
		if _, ok := resultSeries[name]; !ok {
			result.Series[name] = s
		}
	}

	return result, nil
}

// Aggregate performs the specified aggregation on the DataFrame
func (df *DataFrame) Aggregate(column string, aggType AggregationType) (*DataFrame, error) {
	if df == nil || df.Series == nil {
		return nil, fmt.Errorf("DataFrame is nil or empty")
	}

	series, ok := df.Series[column]
	if !ok {
		return nil, fmt.Errorf("column %s not found", column)
	}

	if df.GroupIndices == nil {
		return nil, fmt.Errorf("DataFrame is not grouped")
	}

	// Fast streaming path: single grouping column, avoid GroupIndices slices
	if len(df.GroupColumns) == 1 {
		keyCol := df.GroupColumns[0]
		keySeries := df.Series[keyCol]

		// Build aggregation map keyed by the grouping column's underlying type.
		switch keys := keySeries.Data.(type) {
		case []int64:
			return aggregateStreamingInt64Key(df, keys, series, column, aggType)
		case []string:
			return aggregateStreamingStringKey(df, keys, series, column, aggType)
		case []float64:
			// Using float64 as map key is okay here because we originated them; NaN treated as distinct.
			return aggregateStreamingFloat64Key(df, keys, series, column, aggType)
		case []bool:
			return aggregateStreamingBoolKey(df, keys, series, column, aggType)
		default:
			// Fallback to existing logic
		}
	}

	// Create result series with group columns only
	resultSeries := make(map[string]*Series)
	for name, s := range df.Series {
		if s.Length == df.Length {
			resultSeries[name] = s
		}
	}

	// Build deterministic slice of group keys (same order as group column)
	keys := make([]string, 0, len(df.GroupIndices))
	for k := range df.GroupIndices {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	switch data := series.Data.(type) {
	case []int64:
		newData := make([]int64, len(keys))
		var wg sync.WaitGroup
		for idx, key := range keys {
			indices := df.GroupIndices[key]
			wg.Add(1)
			go func(outIdx int, idxs []int) {
				defer wg.Done()
				if len(idxs) == 0 {
					return
				}
				var res int64
				switch aggType {
				case Sum:
					for _, id := range idxs {
						res += data[id]
					}
				case Mean:
					for _, id := range idxs {
						res += data[id]
					}
					res /= int64(len(idxs))
				case Count:
					res = int64(len(idxs))
				case Min:
					min := data[idxs[0]]
					for _, id := range idxs {
						if data[id] < min {
							min = data[id]
						}
					}
					res = min
				case Max:
					max := data[idxs[0]]
					for _, id := range idxs {
						if data[id] > max {
							max = data[id]
						}
					}
					res = max
				}
				newData[outIdx] = res
			}(idx, indices)
		}
		wg.Wait()
		resultSeries[column] = NewSeries(column, newData)
	case []float64:
		newData := make([]float64, len(keys))
		var wg sync.WaitGroup
		for idx, key := range keys {
			indices := df.GroupIndices[key]
			wg.Add(1)
			go func(outIdx int, idxs []int) {
				defer wg.Done()
				if len(idxs) == 0 {
					return
				}
				var res float64
				switch aggType {
				case Sum:
					for _, id := range idxs {
						res += data[id]
					}
				case Mean:
					for _, id := range idxs {
						res += data[id]
					}
					res /= float64(len(idxs))
				case Count:
					res = float64(len(idxs))
				case Min:
					min := data[idxs[0]]
					for _, id := range idxs {
						if data[id] < min {
							min = data[id]
						}
					}
					res = min
				case Max:
					max := data[idxs[0]]
					for _, id := range idxs {
						if data[id] > max {
							max = data[id]
						}
					}
					res = max
				}
				newData[outIdx] = res
			}(idx, indices)
		}
		wg.Wait()
		resultSeries[column] = NewSeries(column, newData)
	default:
		return nil, fmt.Errorf("unsupported data type for aggregation")
	}

	return New(resultSeries)
}

// SortByColumn sorts the DataFrame by the specified column
func (df *DataFrame) SortByColumn(column string, ascending bool) (*DataFrame, error) {
	series, ok := df.Series[column]
	if !ok {
		return nil, fmt.Errorf("column %s not found", column)
	}

	// Create index slice to track original positions
	indices := make([]int, df.Length)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on the column values
	switch data := series.Data.(type) {
	case []int64:
		sort.Slice(indices, func(i, j int) bool {
			if ascending {
				return data[indices[i]] < data[indices[j]]
			}
			return data[indices[i]] > data[indices[j]]
		})
	case []float64:
		sort.Slice(indices, func(i, j int) bool {
			if ascending {
				return data[indices[i]] < data[indices[j]]
			}
			return data[indices[i]] > data[indices[j]]
		})
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

	// Create new sorted series concurrently for better throughput on wide DataFrames
	sorted := make(map[string]*Series, len(df.Series))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for name, s := range df.Series {
		wg.Add(1)
		go func(name string, s *Series) {
			defer wg.Done()
			switch data := s.Data.(type) {
			case []int64:
				newData := make([]int64, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				series := NewSeries(name, newData)
				mu.Lock()
				sorted[name] = series
				mu.Unlock()
			case []float64:
				newData := make([]float64, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				series := NewSeries(name, newData)
				mu.Lock()
				sorted[name] = series
				mu.Unlock()
			case []string:
				newData := make([]string, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				series := NewSeries(name, newData)
				mu.Lock()
				sorted[name] = series
				mu.Unlock()
			case []bool:
				newData := make([]bool, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				series := NewSeries(name, newData)
				mu.Lock()
				sorted[name] = series
				mu.Unlock()
			}
		}(name, s)
	}

	wg.Wait()

	return New(sorted)
}

// SortByIndex sorts the DataFrame by the row index
func (df *DataFrame) SortByIndex(ascending bool) (*DataFrame, error) {
	// Create index slice
	indices := make([]int, df.Length)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices
	if ascending {
		sort.Ints(indices)
	} else {
		sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	}

	// Create new sorted series concurrently
	sorted := make(map[string]*Series, len(df.Series))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for name, s := range df.Series {
		wg.Add(1)
		go func(name string, s *Series) {
			defer wg.Done()
			switch data := s.Data.(type) {
			case []int64:
				newData := make([]int64, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				mu.Lock()
				sorted[name] = NewSeries(name, newData)
				mu.Unlock()
			case []float64:
				newData := make([]float64, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				mu.Lock()
				sorted[name] = NewSeries(name, newData)
				mu.Unlock()
			case []string:
				newData := make([]string, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				mu.Lock()
				sorted[name] = NewSeries(name, newData)
				mu.Unlock()
			case []bool:
				newData := make([]bool, df.Length)
				for newIdx, oldIdx := range indices {
					newData[newIdx] = data[oldIdx]
				}
				mu.Lock()
				sorted[name] = NewSeries(name, newData)
				mu.Unlock()
			}
		}(name, s)
	}

	wg.Wait()

	return New(sorted)
}

// --- streaming aggregation helpers -------------------------------------------------

type aggStateInt64 struct {
	sum, min, max int64
	count         int64
}
type aggStateFloat64 struct {
	sum, min, max float64
	count         int64
}

func aggregateStreamingInt64Key(df *DataFrame, keys []int64, valSeries *Series, column string, aggType AggregationType) (*DataFrame, error) {
	switch values := valSeries.Data.(type) {
	case []int64:
		state := make(map[int64]*aggStateInt64, len(df.GroupIndices))
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateInt64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}

		// Build result slices in deterministic order (sort keys)
		uniq := make([]int64, 0, len(state))
		for k := range state {
			uniq = append(uniq, k)
		}
		sort.Slice(uniq, func(i, j int) bool { return uniq[i] < uniq[j] })

		resultVals := make([]int64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				if st.count > 0 {
					resultVals[i] = st.sum / st.count
				}
			case Count:
				resultVals[i] = st.count
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}

		// Build group column data slice (keys)
		keySeries := NewSeries(df.GroupColumns[0], uniq)
		resSeries := map[string]*Series{
			df.GroupColumns[0]: keySeries,
			column:             NewSeries(column, resultVals),
		}

		// Attach other original series by reference
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}

		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil

	case []float64:
		state := make(map[int64]*aggStateFloat64, len(df.GroupIndices))
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateFloat64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := make([]int64, 0, len(state))
		for k := range state {
			uniq = append(uniq, k)
		}
		sort.Slice(uniq, func(i, j int) bool { return uniq[i] < uniq[j] })

		resultVals := make([]float64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				if st.count > 0 {
					resultVals[i] = st.sum / float64(st.count)
				}
			case Count:
				resultVals[i] = float64(st.count)
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}

		keySeries := NewSeries(df.GroupColumns[0], uniq)
		resSeries := map[string]*Series{
			df.GroupColumns[0]: keySeries,
			column:             NewSeries(column, resultVals),
		}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil

	default:
		return nil, fmt.Errorf("unsupported aggregation data type for streaming path")
	}
}

func aggregateStreamingStringKey(df *DataFrame, keys []string, valSeries *Series, column string, aggType AggregationType) (*DataFrame, error) {
	// Similar logic but keys are strings.
	switch values := valSeries.Data.(type) {
	case []int64:
		state := make(map[string]*aggStateInt64)
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateInt64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := make([]string, 0, len(state))
		for k := range state {
			uniq = append(uniq, k)
		}
		sort.Strings(uniq)
		resultVals := make([]int64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				if st.count > 0 {
					resultVals[i] = st.sum / st.count
				}
			case Count:
				resultVals[i] = st.count
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}
		resSeries := map[string]*Series{
			df.GroupColumns[0]: NewSeries(df.GroupColumns[0], uniq),
			column:             NewSeries(column, resultVals),
		}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil
	case []float64:
		state := make(map[string]*aggStateFloat64)
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateFloat64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := make([]string, 0, len(state))
		for k := range state {
			uniq = append(uniq, k)
		}
		sort.Strings(uniq)
		resultVals := make([]float64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				if st.count > 0 {
					resultVals[i] = st.sum / float64(st.count)
				}
			case Count:
				resultVals[i] = float64(st.count)
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}
		resSeries := map[string]*Series{df.GroupColumns[0]: NewSeries(df.GroupColumns[0], uniq), column: NewSeries(column, resultVals)}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil
	default:
		return nil, fmt.Errorf("unsupported data type for streaming path")
	}
}

func aggregateStreamingFloat64Key(df *DataFrame, keys []float64, valSeries *Series, column string, aggType AggregationType) (*DataFrame, error) {
	// Convert float64 key to string for sorting stability
	stateInt := make(map[float64]*aggStateFloat64)
	switch values := valSeries.Data.(type) {
	case []float64:
		for i, k := range keys {
			v := values[i]
			s, ok := stateInt[k]
			if !ok {
				stateInt[k] = &aggStateFloat64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := make([]float64, 0, len(stateInt))
		for k := range stateInt {
			uniq = append(uniq, k)
		}
		sort.Float64s(uniq)
		resultVals := make([]float64, len(uniq))
		for i, k := range uniq {
			st := stateInt[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				if st.count > 0 {
					resultVals[i] = st.sum / float64(st.count)
				}
			case Count:
				resultVals[i] = float64(st.count)
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}
		resSeries := map[string]*Series{df.GroupColumns[0]: NewSeries(df.GroupColumns[0], uniq), column: NewSeries(column, resultVals)}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil
	default:
		return nil, fmt.Errorf("unsupported data type for streaming float64 key path")
	}
}

func aggregateStreamingBoolKey(df *DataFrame, keys []bool, valSeries *Series, column string, aggType AggregationType) (*DataFrame, error) {
	// keys are bool -> map[bool]
	switch values := valSeries.Data.(type) {
	case []int64:
		state := map[bool]*aggStateInt64{}
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateInt64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := []bool{}
		if _, ok := state[false]; ok {
			uniq = append(uniq, false)
		}
		if _, ok := state[true]; ok {
			uniq = append(uniq, true)
		}
		resultVals := make([]int64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				resultVals[i] = st.sum / st.count
			case Count:
				resultVals[i] = st.count
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}
		resSeries := map[string]*Series{df.GroupColumns[0]: NewSeries(df.GroupColumns[0], uniq), column: NewSeries(column, resultVals)}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil
	case []float64:
		state := map[bool]*aggStateFloat64{}
		for i, k := range keys {
			v := values[i]
			s, ok := state[k]
			if !ok {
				state[k] = &aggStateFloat64{sum: v, min: v, max: v, count: 1}
			} else {
				s.sum += v
				s.count++
				if v < s.min {
					s.min = v
				}
				if v > s.max {
					s.max = v
				}
			}
		}
		uniq := []bool{}
		if _, ok := state[false]; ok {
			uniq = append(uniq, false)
		}
		if _, ok := state[true]; ok {
			uniq = append(uniq, true)
		}
		resultVals := make([]float64, len(uniq))
		for i, k := range uniq {
			st := state[k]
			switch aggType {
			case Sum:
				resultVals[i] = st.sum
			case Mean:
				resultVals[i] = st.sum / float64(st.count)
			case Count:
				resultVals[i] = float64(st.count)
			case Min:
				resultVals[i] = st.min
			case Max:
				resultVals[i] = st.max
			}
		}
		resSeries := map[string]*Series{df.GroupColumns[0]: NewSeries(df.GroupColumns[0], uniq), column: NewSeries(column, resultVals)}
		for name, s := range df.Series {
			if name != df.GroupColumns[0] && name != column {
				resSeries[name] = s
			}
		}
		return &DataFrame{Series: resSeries, Length: len(uniq), GroupIndices: nil, GroupColumns: df.GroupColumns}, nil
	default:
		return nil, fmt.Errorf("unsupported data type for streaming bool path")
	}
}
