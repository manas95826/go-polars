package main

/*
#include <stdlib.h>
#include <stdint.h>

// Export these symbols without underscore prefix
int64_t NewDataFrame(void);
int AddSeries(int64_t handle, char* name, void* data, int length, int dtype);
int GetShape(int64_t handle, int* rows, int* cols);
void DeleteDataFrame(int64_t handle);
int SortByColumn(int64_t handle, char* column, int ascending);
int SortByIndex(int64_t handle, int ascending);
int64_t GroupBy(int64_t handle, char** columns, int num_columns);
int64_t Aggregate(int64_t handle, char* column, int agg_type);
*/
import "C"
import (
	"unsafe"

	"go-polars/types"
)

// Handle type for DataFrame to pass between Go and C
type Handle struct {
	df     *types.DataFrame
	series map[string]*types.Series
}

var handles = make(map[C.int64_t]*Handle)
var nextHandle C.int64_t = 1

//export NewDataFrame
func NewDataFrame() C.int64_t {
	df, err := types.New(make(map[string]*types.Series))
	if err != nil {
		return -1
	}

	handle := nextHandle
	nextHandle++
	handles[handle] = &Handle{
		df:     df,
		series: make(map[string]*types.Series),
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

	var series *types.Series
	switch dtype {
	case 0: // int64
		slice := unsafe.Slice((*int64)(data), goLength)
		series = types.NewSeries(goName, slice)
	case 1: // float64
		slice := unsafe.Slice((*float64)(data), goLength)
		series = types.NewSeries(goName, slice)
	case 2: // bool
		slice := unsafe.Slice((*bool)(data), goLength)
		series = types.NewSeries(goName, slice)
	default:
		return -1
	}

	// Add series to our map
	h.series[goName] = series

	// Create new DataFrame with all series
	newDf, err := types.New(h.series)
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

//export SortByColumn
func SortByColumn(handle C.int64_t, column *C.char, ascending C.int) C.int {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	goColumn := C.GoString(column)
	goAscending := ascending != 0

	df, err := h.df.SortByColumn(goColumn, goAscending)
	if err != nil {
		return -1
	}

	h.df = df
	return 0
}

//export SortByIndex
func SortByIndex(handle C.int64_t, ascending C.int) C.int {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	goAscending := ascending != 0

	df, err := h.df.SortByIndex(goAscending)
	if err != nil {
		return -1
	}

	h.df = df
	return 0
}

//export GroupBy
func GroupBy(handle C.int64_t, columns **C.char, numColumns C.int) C.int64_t {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	// Convert C string array to Go string slice
	goColumns := make([]string, int(numColumns))
	cColumns := (*[1 << 30]*C.char)(unsafe.Pointer(columns))[:numColumns:numColumns]
	for i, cStr := range cColumns {
		goColumns[i] = C.GoString(cStr)
	}

	// Group the DataFrame
	grouped, err := h.df.GroupBy(goColumns)
	if err != nil {
		return -1
	}

	// Create new handle for grouped DataFrame
	newHandle := nextHandle
	nextHandle++
	handles[newHandle] = &Handle{
		df:     grouped,
		series: grouped.Series,
	}
	return newHandle
}

//export Aggregate
func Aggregate(handle C.int64_t, column *C.char, aggType C.int) C.int64_t {
	h, ok := handles[handle]
	if !ok {
		return -1
	}

	goColumn := C.GoString(column)
	goAggType := types.AggregationType(aggType)

	df, err := h.df.Aggregate(goColumn, goAggType)
	if err != nil {
		return -1
	}

	// Create new handle for aggregated DataFrame
	newHandle := nextHandle
	nextHandle++
	handles[newHandle] = &Handle{
		df:     df,
		series: df.Series,
	}
	return newHandle
}

func main() {}
