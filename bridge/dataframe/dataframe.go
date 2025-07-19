package dataframe

import (
	"fmt"

	"go-polars/types"
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
