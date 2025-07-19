package main

/*
#include <stdlib.h>
#include <stdint.h>

// Export these symbols without underscore prefix
int64_t NewDataFrame(void);
int AddSeries(int64_t handle, char* name, void* data, int length, int dtype);
int GetShape(int64_t handle, int* rows, int* cols);
void DeleteDataFrame(int64_t handle);
int64_t SortByColumn(int64_t handle, char* column, int ascending);
int64_t SortByIndex(int64_t handle, int ascending);
int64_t GroupBy(int64_t handle, char** columns, int num_columns);
int64_t Aggregate(int64_t handle, char* column, int agg_type);
int64_t Head(int64_t handle, int n);
void* GetSeries(int64_t handle, char* name, int* length, int* dtype);
char* GetColumn(int64_t handle, int index);
int GetColumnCount(int64_t handle);
*/
import "C"
import (
	"unsafe"

	"go-polars/types"
)

// Handle represents a DataFrame held by Go but referenced from C/Python.
type Handle struct {
	df     *types.DataFrame
	series map[string]*types.Series // owns its own copy so it never gets stale
}

var (
	handles              = make(map[C.int64_t]*Handle)
	nextHandle C.int64_t = 1
)

// newHandleFrom copies df.Series and registers a fresh Handle.
func newHandleFrom(df *types.DataFrame) C.int64_t {
	fresh := make(map[string]*types.Series, len(df.Series))
	for k, v := range df.Series {
		fresh[k] = v
	}
	id := nextHandle
	nextHandle++
	handles[id] = &Handle{df: df, series: fresh}
	return id
}

//export NewDataFrame
func NewDataFrame() C.int64_t {
	df, err := types.New(make(map[string]*types.Series))
	if err != nil {
		return -1
	}
	return newHandleFrom(df)
}

//export AddSeries
func AddSeries(hID C.int64_t, name *C.char, data unsafe.Pointer, length C.int, dtype C.int) C.int {
	h, ok := handles[hID]
	if !ok {
		return -1
	}

	goName := C.GoString(name)
	goLen := int(length)

	var s *types.Series
	switch dtype {
	case 0:
		s = types.NewSeries(goName, unsafe.Slice((*int64)(data), goLen))
	case 1:
		s = types.NewSeries(goName, unsafe.Slice((*float64)(data), goLen))
	case 2:
		s = types.NewSeries(goName, unsafe.Slice((*bool)(data), goLen))
	default:
		return -1
	}

	h.series[goName] = s
	newDF, err := types.New(h.series)
	if err != nil {
		return -1
	}
	h.df = newDF
	return 0
}

//export GetShape
func GetShape(hID C.int64_t, rows, cols *C.int) C.int {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	r, c := h.df.Shape()
	*rows = C.int(r)
	*cols = C.int(c)
	return 0
}

//export DeleteDataFrame
func DeleteDataFrame(hID C.int64_t) { delete(handles, hID) }

//export SortByColumn
func SortByColumn(hID C.int64_t, column *C.char, asc C.int) C.int64_t {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	res, err := h.df.SortByColumn(C.GoString(column), asc != 0)
	if err != nil {
		return -1
	}
	return newHandleFrom(res)
}

//export SortByIndex
func SortByIndex(hID C.int64_t, asc C.int) C.int64_t {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	res, err := h.df.SortByIndex(asc != 0)
	if err != nil {
		return -1
	}
	return newHandleFrom(res)
}

//export GroupBy
func GroupBy(hID C.int64_t, cols **C.char, n C.int) C.int64_t {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	goCols := make([]string, int(n))
	cSlice := (*[1 << 30]*C.char)(unsafe.Pointer(cols))[:n:n]
	for i, c := range cSlice {
		goCols[i] = C.GoString(c)
	}
	res, err := h.df.GroupBy(goCols)
	if err != nil {
		return -1
	}
	return newHandleFrom(res)
}

//export Aggregate
func Aggregate(hID C.int64_t, column *C.char, agg C.int) C.int64_t {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	res, err := h.df.Aggregate(C.GoString(column), types.AggregationType(agg))
	if err != nil {
		return -1
	}
	return newHandleFrom(res)
}

//export Head
func Head(hID C.int64_t, n C.int) C.int64_t {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	res, err := h.df.Head(int(n))
	if err != nil {
		return -1
	}
	return newHandleFrom(res)
}

//export GetColumnCount
func GetColumnCount(hID C.int64_t) C.int {
	h, ok := handles[hID]
	if !ok {
		return -1
	}
	return C.int(len(h.df.Columns()))
}

//export GetColumn
func GetColumn(hID C.int64_t, idx C.int) *C.char {
	h, ok := handles[hID]
	if !ok {
		return nil
	}
	cols := h.df.Columns()
	if int(idx) >= len(cols) {
		return nil
	}
	return C.CString(cols[idx])
}

//export GetSeries
func GetSeries(hID C.int64_t, name *C.char, length, dtype *C.int) unsafe.Pointer {
	h, ok := handles[hID]
	if !ok {
		return nil
	}
	series, ok := h.df.Series[C.GoString(name)]
	if !ok {
		return nil
	}
	switch data := series.Data.(type) {
	case []int64:
		*length, *dtype = C.int(len(data)), 0
		return unsafe.Pointer(&data[0])
	case []float64:
		*length, *dtype = C.int(len(data)), 1
		return unsafe.Pointer(&data[0])
	case []bool:
		*length, *dtype = C.int(len(data)), 2
		return unsafe.Pointer(&data[0])
	default:
		return nil
	}
}

func main() {}
