/* Code generated by cmd/cgo; DO NOT EDIT. */

/* package command-line-arguments */


#line 1 "cgo-builtin-export-prolog"

#include <stddef.h>

#ifndef GO_CGO_EXPORT_PROLOGUE_H
#define GO_CGO_EXPORT_PROLOGUE_H

#ifndef GO_CGO_GOSTRING_TYPEDEF
typedef struct { const char *p; ptrdiff_t n; } _GoString_;
#endif

#endif

/* Start of preamble from import "C" comments.  */


#line 3 "bridge.go"

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

#line 1 "cgo-generated-wrapper"


/* End of preamble from import "C" comments.  */


/* Start of boilerplate cgo prologue.  */
#line 1 "cgo-gcc-export-header-prolog"

#ifndef GO_CGO_PROLOGUE_H
#define GO_CGO_PROLOGUE_H

typedef signed char GoInt8;
typedef unsigned char GoUint8;
typedef short GoInt16;
typedef unsigned short GoUint16;
typedef int GoInt32;
typedef unsigned int GoUint32;
typedef long long GoInt64;
typedef unsigned long long GoUint64;
typedef GoInt64 GoInt;
typedef GoUint64 GoUint;
typedef size_t GoUintptr;
typedef float GoFloat32;
typedef double GoFloat64;
#ifdef _MSC_VER
#include <complex.h>
typedef _Fcomplex GoComplex64;
typedef _Dcomplex GoComplex128;
#else
typedef float _Complex GoComplex64;
typedef double _Complex GoComplex128;
#endif

/*
  static assertion to make sure the file is being used on architecture
  at least with matching size of GoInt.
*/
typedef char _check_for_64_bit_pointer_matching_GoInt[sizeof(void*)==64/8 ? 1:-1];

#ifndef GO_CGO_GOSTRING_TYPEDEF
typedef _GoString_ GoString;
#endif
typedef void *GoMap;
typedef void *GoChan;
typedef struct { void *t; void *v; } GoInterface;
typedef struct { void *data; GoInt len; GoInt cap; } GoSlice;

#endif

/* End of boilerplate cgo prologue.  */

#ifdef __cplusplus
extern "C" {
#endif

extern int64_t NewDataFrame();
extern int AddSeries(int64_t hID, char* name, void* data, int length, int dtype);
extern int GetShape(int64_t hID, int* rows, int* cols);
extern void DeleteDataFrame(int64_t hID);
extern int64_t SortByColumn(int64_t hID, char* column, int asc);
extern int64_t SortByIndex(int64_t hID, int asc);
extern int64_t GroupBy(int64_t hID, char** cols, int n);
extern int64_t Aggregate(int64_t hID, char* column, int agg);
extern int64_t Head(int64_t hID, int n);
extern int GetColumnCount(int64_t hID);
extern char* GetColumn(int64_t hID, int idx);
extern void* GetSeries(int64_t hID, char* name, int* length, int* dtype);

#ifdef __cplusplus
}
#endif
