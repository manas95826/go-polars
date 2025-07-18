package main

// #include <stdlib.h>
// #include <stdint.h>
import "C"
import (
	"fmt"
	"unsafe"
)

// Series represents a column of data with a name and type
type Series struct {
	Name   string
	Data   interface{}
	Length int
}

// NewSeries creates a new Series from a slice of data
func NewSeries(name string, data interface{}) *Series {
	var length int
	switch d := data.(type) {
	case []int64:
		length = len(d)
	case []float64:
		length = len(d)
	case []string:
		length = len(d)
	case []bool:
		length = len(d)
	default:
		return nil
	}

	return &Series{
		Name:   name,
		Data:   data,
		Length: length,
	}
}

// DataFrame represents a collection of Series with the same length
type DataFrame struct {
	series map[string]*Series
	length int
}

// New creates a new DataFrame from a map of Series
func New(series map[string]*Series) (*DataFrame, error) {
	if len(series) == 0 {
		return &DataFrame{
			series: make(map[string]*Series),
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

// Shape returns the dimensions of the DataFrame (rows, columns)
func (df *DataFrame) Shape() (int, int) {
	return df.length, len(df.series)
}

// Handle type for DataFrame to pass between Go and C
type Handle struct {
	series map[string]*Series
	df     *DataFrame
}

var handles = make(map[C.int64_t]*Handle)
var nextHandle C.int64_t = 1

//export NewDataFrame
func NewDataFrame() C.int64_t {
	df, err := New(make(map[string]*Series))
	if err != nil {
		return -1
	}

	handle := nextHandle
	nextHandle++
	handles[handle] = &Handle{
		df:     df,
		series: make(map[string]*Series),
	}
	return handle
}

//export AddSeries
func AddSeries(handle C.int64_t, name *C.char, data unsafe.Pointer, length C.int, dtype C.int) C.int {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	goName := C.GoString(name)
	goLength := int(length)

	var series *Series
	switch dtype {
	case 0: // int64
		slice := unsafe.Slice((*int64)(data), goLength)
		series = NewSeries(goName, slice)
	case 1: // float64
		slice := unsafe.Slice((*float64)(data), goLength)
		series = NewSeries(goName, slice)
	case 2: // bool
		slice := unsafe.Slice((*bool)(data), goLength)
		series = NewSeries(goName, slice)
	default:
		return -1
	}

	// Add series to our map
	h.series[goName] = series

	// Create new DataFrame with all series
	newDf, err := New(h.series)
	if err != nil {
		return -1
	}
	h.df = newDf
	return 0
}

//export GetShape
func GetShape(handle C.int64_t, rows *C.int, cols *C.int) C.int {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	r, c := h.df.Shape()
	*rows = C.int(r)
	*cols = C.int(c)
	return 0
}

//export DeleteDataFrame
func DeleteDataFrame(handle C.int64_t) {
	delete(handles, handle)
}

func main() {}
