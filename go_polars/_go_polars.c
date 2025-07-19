#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "numpy/arrayobject.h"
#include <stdint.h>

#ifdef __APPLE__
    #define LIB_NAME "libgo_polars.dylib"
#elif _WIN32
    #define LIB_NAME "go_polars.dll"
#else
    #define LIB_NAME "libgo_polars.so"
#endif

// Forward declarations of types
static PyTypeObject DataFrameType;
static PyTypeObject GroupedDataFrameType;

// Function declarations from Go - no underscore prefix
extern int64_t NewDataFrame(void);
extern int AddSeries(int64_t handle, const char* name, void* data, int length, int dtype);
extern int GetShape(int64_t handle, int* rows, int* cols);
extern void DeleteDataFrame(int64_t handle);
extern int SortByColumn(int64_t handle, const char* column, int ascending);
extern int SortByIndex(int64_t handle, int ascending);
extern int64_t GroupBy(int64_t handle, const char** columns, int num_columns);
extern int Aggregate(int64_t handle, const char* column, int agg_type);

typedef struct {
    PyObject_HEAD
    int64_t handle;
} DataFrameObject;

typedef struct {
    PyObject_HEAD
    int64_t handle;
} GroupedDataFrameObject;

static void
DataFrame_dealloc(DataFrameObject *self)
{
    DeleteDataFrame(self->handle);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *
DataFrame_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    DataFrameObject *self;
    self = (DataFrameObject *) type->tp_alloc(type, 0);
    if (self != NULL) {
        self->handle = NewDataFrame();
        if (self->handle == -1) {
            Py_DECREF(self);
            PyErr_SetString(PyExc_RuntimeError, "Failed to create DataFrame");
            return NULL;
        }
    }
    return (PyObject *) self;
}

static PyObject *
DataFrame_add_series(DataFrameObject *self, PyObject *args)
{
    const char *name;
    PyObject *array;
    
    if (!PyArg_ParseTuple(args, "sO", &name, &array)) {
        return NULL;
    }

    if (!PyArray_Check(array)) {
        PyErr_SetString(PyExc_TypeError, "Expected numpy array");
        return NULL;
    }

    PyArrayObject *arr = (PyArrayObject *)array;
    int dtype;
    switch(PyArray_TYPE(arr)) {
        case NPY_INT64:
            dtype = 0;
            break;
        case NPY_FLOAT64:
            dtype = 1;
            break;
        case NPY_BOOL:
            dtype = 2;
            break;
        default:
            PyErr_SetString(PyExc_TypeError, "Unsupported dtype");
            return NULL;
    }

    int result = AddSeries(
        self->handle,
        name,
        PyArray_DATA(arr),
        (int)PyArray_SIZE(arr),
        dtype
    );

    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add series");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
DataFrame_shape(DataFrameObject *self, PyObject *Py_UNUSED(ignored))
{
    int rows, cols;
    if (GetShape(self->handle, &rows, &cols) != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get shape");
        return NULL;
    }
    return Py_BuildValue("(ii)", rows, cols);
}

static PyObject *
DataFrame_from_dict(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *data;
    static char *kwlist[] = {"data", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &data)) {
        return NULL;
    }

    if (!PyDict_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "Expected dictionary");
        return NULL;
    }

    // Create new DataFrame
    PyObject *df = DataFrame_new(type, NULL, NULL);
    if (df == NULL) {
        return NULL;
    }

    // Add each series
    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(data, &pos, &key, &value)) {
        if (!PyUnicode_Check(key)) {
            PyErr_SetString(PyExc_TypeError, "Dictionary keys must be strings");
            Py_DECREF(df);
            return NULL;
        }

        PyObject *args = Py_BuildValue("(OO)", key, value);
        if (args == NULL) {
            Py_DECREF(df);
            return NULL;
        }

        PyObject *result = DataFrame_add_series((DataFrameObject *)df, args);
        Py_DECREF(args);

        if (result == NULL) {
            Py_DECREF(df);
            return NULL;
        }
        Py_DECREF(result);
    }

    return df;
}

static PyObject *
DataFrame_sort_by_column(DataFrameObject *self, PyObject *args)
{
    const char *column;
    int ascending = 1;  // default to True
    
    if (!PyArg_ParseTuple(args, "s|p", &column, &ascending)) {
        return NULL;
    }

    int result = SortByColumn(self->handle, column, ascending);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to sort DataFrame");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
DataFrame_sort_by_index(DataFrameObject *self, PyObject *args)
{
    int ascending = 1;  // default to True
    
    if (!PyArg_ParseTuple(args, "|p", &ascending)) {
        return NULL;
    }

    int result = SortByIndex(self->handle, ascending);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to sort DataFrame");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
DataFrame_group_by(DataFrameObject *self, PyObject *args)
{
    PyObject *columns_list;
    
    if (!PyArg_ParseTuple(args, "O", &columns_list)) {
        return NULL;
    }

    if (!PyList_Check(columns_list)) {
        PyErr_SetString(PyExc_TypeError, "Expected list of column names");
        return NULL;
    }

    Py_ssize_t num_columns = PyList_Size(columns_list);
    const char **columns = malloc(num_columns * sizeof(char*));
    if (!columns) {
        PyErr_NoMemory();
        return NULL;
    }

    for (Py_ssize_t i = 0; i < num_columns; i++) {
        PyObject *item = PyList_GetItem(columns_list, i);
        if (!PyUnicode_Check(item)) {
            free(columns);
            PyErr_SetString(PyExc_TypeError, "Column names must be strings");
            return NULL;
        }
        columns[i] = PyUnicode_AsUTF8(item);
    }

    int64_t grouped_handle = GroupBy(self->handle, columns, (int)num_columns);
    free(columns);

    if (grouped_handle == -1) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to group DataFrame");
        return NULL;
    }

    GroupedDataFrameObject *grouped = (GroupedDataFrameObject*)PyType_GenericNew(&GroupedDataFrameType, NULL, NULL);
    if (!grouped) {
        return NULL;
    }
    grouped->handle = grouped_handle;

    return (PyObject*)grouped;
}

static void
GroupedDataFrame_dealloc(GroupedDataFrameObject *self)
{
    DeleteDataFrame(self->handle);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *
GroupedDataFrame_aggregate(GroupedDataFrameObject *self, PyObject *args)
{
    const char *column;
    int agg_type;
    
    if (!PyArg_ParseTuple(args, "si", &column, &agg_type)) {
        return NULL;
    }

    int64_t result = Aggregate(self->handle, column, agg_type);
    if (result == -1) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to aggregate DataFrame");
        return NULL;
    }

    DataFrameObject *df = (DataFrameObject*)PyType_GenericNew(&DataFrameType, NULL, NULL);
    if (!df) {
        return NULL;
    }
    df->handle = result;

    return (PyObject*)df;
}

static PyMethodDef GroupedDataFrame_methods[] = {
    {"aggregate", (PyCFunction) GroupedDataFrame_aggregate, METH_VARARGS,
     "Aggregate the grouped DataFrame"},
    {NULL}  /* Sentinel */
};

static PyTypeObject GroupedDataFrameType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "go_polars._go_polars.GroupedDataFrame",
    .tp_doc = "GroupedDataFrame object",
    .tp_basicsize = sizeof(GroupedDataFrameObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor) GroupedDataFrame_dealloc,
    .tp_methods = GroupedDataFrame_methods,
    .tp_new = PyType_GenericNew,
};

static PyMethodDef DataFrame_methods[] = {
    {"add_series", (PyCFunction) DataFrame_add_series, METH_VARARGS,
     "Add a series to the DataFrame"},
    {"shape", (PyCFunction) DataFrame_shape, METH_NOARGS,
     "Get the shape of the DataFrame"},
    {"sort_by_column", (PyCFunction) DataFrame_sort_by_column, METH_VARARGS,
     "Sort DataFrame by column"},
    {"sort_by_index", (PyCFunction) DataFrame_sort_by_index, METH_VARARGS,
     "Sort DataFrame by index"},
    {"group_by", (PyCFunction) DataFrame_group_by, METH_VARARGS,
     "Group DataFrame by columns"},
    {NULL}  /* Sentinel */
};

static PyTypeObject DataFrameType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "go_polars._go_polars.DataFrame",
    .tp_doc = "DataFrame object",
    .tp_basicsize = sizeof(DataFrameObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = DataFrame_new,
    .tp_dealloc = (destructor) DataFrame_dealloc,
    .tp_methods = DataFrame_methods,
};

static PyModuleDef go_polarsmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "go_polars._go_polars",
    .m_doc = "Python interface for go-polars.",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit__go_polars(void)
{
    import_array();  // Initialize NumPy

    PyObject *m;
    if (PyType_Ready(&DataFrameType) < 0)
        return NULL;

    if (PyType_Ready(&GroupedDataFrameType) < 0)
        return NULL;

    m = PyModule_Create(&go_polarsmodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&DataFrameType);
    if (PyModule_AddObject(m, "DataFrame", (PyObject *) &DataFrameType) < 0) {
        Py_DECREF(&DataFrameType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&GroupedDataFrameType);
    if (PyModule_AddObject(m, "GroupedDataFrame", (PyObject *) &GroupedDataFrameType) < 0) {
        Py_DECREF(&GroupedDataFrameType);
        Py_DECREF(&DataFrameType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
} 