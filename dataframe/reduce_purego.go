//go:build !simd
// +build !simd

package dataframe

// sumInt64Indexed returns the sum of values at given indices.
func sumInt64Indexed(data []int64, idx []int) int64 {
	var sum int64
	for _, i := range idx {
		sum += data[i]
	}
	return sum
}

// minInt64Indexed returns minimum value across indices.
func minInt64Indexed(data []int64, idx []int) int64 {
	if len(idx) == 0 {
		return 0
	}
	min := data[idx[0]]
	for _, i := range idx[1:] {
		if v := data[i]; v < min {
			min = v
		}
	}
	return min
}

// maxInt64Indexed returns maximum value across indices.
func maxInt64Indexed(data []int64, idx []int) int64 {
	if len(idx) == 0 {
		return 0
	}
	max := data[idx[0]]
	for _, i := range idx[1:] {
		if v := data[i]; v > max {
			max = v
		}
	}
	return max
}

// sumFloat64Indexed returns the sum of float64 values.
func sumFloat64Indexed(data []float64, idx []int) float64 {
	var sum float64
	for _, i := range idx {
		sum += data[i]
	}
	return sum
}

// minFloat64Indexed returns minimum float.
func minFloat64Indexed(data []float64, idx []int) float64 {
	if len(idx) == 0 {
		return 0
	}
	min := data[idx[0]]
	for _, i := range idx[1:] {
		if v := data[i]; v < min {
			min = v
		}
	}
	return min
}

// maxFloat64Indexed returns maximum float.
func maxFloat64Indexed(data []float64, idx []int) float64 {
	if len(idx) == 0 {
		return 0
	}
	max := data[idx[0]]
	for _, i := range idx[1:] {
		if v := data[i]; v > max {
			max = v
		}
	}
	return max
}
