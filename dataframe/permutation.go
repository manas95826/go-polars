package dataframe

// inPlacePermuteInt64 permutes data according to indices slice.
func inPlacePermuteInt64(data []int64, idx []int) {
	visited := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		if visited[i] || idx[i] == i {
			continue
		}
		j := i
		for !visited[j] {
			visited[j] = true
			k := idx[j]
			if k == j {
				break
			}
			data[j], data[k] = data[k], data[j]
			j = k
		}
	}
}

func inPlacePermuteFloat64(data []float64, idx []int) {
	visited := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		if visited[i] || idx[i] == i {
			continue
		}
		j := i
		for !visited[j] {
			visited[j] = true
			k := idx[j]
			if k == j {
				break
			}
			data[j], data[k] = data[k], data[j]
			j = k
		}
	}
}

func inPlacePermuteString(data []string, idx []int) {
	visited := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		if visited[i] || idx[i] == i {
			continue
		}
		j := i
		for !visited[j] {
			visited[j] = true
			k := idx[j]
			if k == j {
				break
			}
			data[j], data[k] = data[k], data[j]
			j = k
		}
	}
}

func inPlacePermuteBool(data []bool, idx []int) {
	visited := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		if visited[i] || idx[i] == i {
			continue
		}
		j := i
		for !visited[j] {
			visited[j] = true
			k := idx[j]
			if k == j {
				break
			}
			data[j], data[k] = data[k], data[j]
			j = k
		}
	}
}
