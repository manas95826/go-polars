//go:build simd
// +build simd

package dataframe

import "unsafe"

// sumInt64Indexed processes four indices per iteration to leverage ILP/SIMD
func sumInt64Indexed(data []int64, idx []int) int64 {
	var s0, s1, s2, s3 int64
	n := len(idx)
	i := 0
	for ; i+3 < n; i += 4 {
		p := unsafe.Slice(&data[0], len(data))
		s0 += p[idx[i]]
		s1 += p[idx[i+1]]
		s2 += p[idx[i+2]]
		s3 += p[idx[i+3]]
	}
	var tail int64
	for ; i < n; i++ {
		tail += data[idx[i]]
	}
	return (s0 + s1) + (s2 + s3) + tail
}

func minInt64Indexed(data []int64, idx []int) int64 {
	if len(idx) == 0 {
		return 0
	}
	p := unsafe.Slice(&data[0], len(data))
	min0 := p[idx[0]]
	min1, min2, min3 := min0, min0, min0
	n := len(idx)
	i := 1
	for ; i+3 < n; i += 4 {
		v0 := p[idx[i]]
		v1 := p[idx[i+1]]
		v2 := p[idx[i+2]]
		v3 := p[idx[i+3]]
		if v0 < min0 {
			min0 = v0
		}
		if v1 < min1 {
			min1 = v1
		}
		if v2 < min2 {
			min2 = v2
		}
		if v3 < min3 {
			min3 = v3
		}
	}
	for ; i < n; i++ {
		v := p[idx[i]]
		if v < min0 {
			min0 = v
		}
	}
	if min1 < min0 {
		min0 = min1
	}
	if min2 < min0 {
		min0 = min2
	}
	if min3 < min0 {
		min0 = min3
	}
	return min0
}

func maxInt64Indexed(data []int64, idx []int) int64 {
	if len(idx) == 0 {
		return 0
	}
	p := unsafe.Slice(&data[0], len(data))
	max0 := p[idx[0]]
	max1, max2, max3 := max0, max0, max0
	n := len(idx)
	i := 1
	for ; i+3 < n; i += 4 {
		v0 := p[idx[i]]
		v1 := p[idx[i+1]]
		v2 := p[idx[i+2]]
		v3 := p[idx[i+3]]
		if v0 > max0 {
			max0 = v0
		}
		if v1 > max1 {
			max1 = v1
		}
		if v2 > max2 {
			max2 = v2
		}
		if v3 > max3 {
			max3 = v3
		}
	}
	for ; i < n; i++ {
		v := p[idx[i]]
		if v > max0 {
			max0 = v
		}
	}
	if max1 > max0 {
		max0 = max1
	}
	if max2 > max0 {
		max0 = max2
	}
	if max3 > max0 {
		max0 = max3
	}
	return max0
}

// Float64 helpers.
func sumFloat64Indexed(data []float64, idx []int) float64 {
	var s0, s1, s2, s3 float64
	n := len(idx)
	i := 0
	for ; i+3 < n; i += 4 {
		p := unsafe.Slice(&data[0], len(data))
		s0 += p[idx[i]]
		s1 += p[idx[i+1]]
		s2 += p[idx[i+2]]
		s3 += p[idx[i+3]]
	}
	var tail float64
	for ; i < n; i++ {
		tail += data[idx[i]]
	}
	return (s0 + s1) + (s2 + s3) + tail
}

func minFloat64Indexed(data []float64, idx []int) float64 {
	if len(idx) == 0 {
		return 0
	}
	p := unsafe.Slice(&data[0], len(data))
	min0 := p[idx[0]]
	min1, min2, min3 := min0, min0, min0
	n := len(idx)
	i := 1
	for ; i+3 < n; i += 4 {
		v0 := p[idx[i]]
		v1 := p[idx[i+1]]
		v2 := p[idx[i+2]]
		v3 := p[idx[i+3]]
		if v0 < min0 {
			min0 = v0
		}
		if v1 < min1 {
			min1 = v1
		}
		if v2 < min2 {
			min2 = v2
		}
		if v3 < min3 {
			min3 = v3
		}
	}
	for ; i < n; i++ {
		v := p[idx[i]]
		if v < min0 {
			min0 = v
		}
	}
	if min1 < min0 {
		min0 = min1
	}
	if min2 < min0 {
		min0 = min2
	}
	if min3 < min0 {
		min0 = min3
	}
	return min0
}

func maxFloat64Indexed(data []float64, idx []int) float64 {
	if len(idx) == 0 {
		return 0
	}
	p := unsafe.Slice(&data[0], len(data))
	max0 := p[idx[0]]
	max1, max2, max3 := max0, max0, max0
	n := len(idx)
	i := 1
	for ; i+3 < n; i += 4 {
		v0 := p[idx[i]]
		v1 := p[idx[i+1]]
		v2 := p[idx[i+2]]
		v3 := p[idx[i+3]]
		if v0 > max0 {
			max0 = v0
		}
		if v1 > max1 {
			max1 = v1
		}
		if v2 > max2 {
			max2 = v2
		}
		if v3 > max3 {
			max3 = v3
		}
	}
	for ; i < n; i++ {
		v := p[idx[i]]
		if v > max0 {
			max0 = v
		}
	}
	if max1 > max0 {
		max0 = max1
	}
	if max2 > max0 {
		max0 = max2
	}
	if max3 > max0 {
		max0 = max3
	}
	return max0
}
