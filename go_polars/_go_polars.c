#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "numpy/arrayobject.h"
#include <stdint.h>

// Function declarations from Go
int64_t NewDataFrame(void);
int AddSeries(int64_t handle, const char* name, void* data, int length, int dtype);
int GetShape(int64_t handle, int* rows, int* cols);
void DeleteDataFrame(int64_t handle);

typedef struct {
    PyObject_HEAD
    int64_t handle;
} DataFrameObject;

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

static PyMethodDef DataFrame_methods[] = {
    {"add_series", (PyCFunction) DataFrame_add_series, METH_VARARGS,
     "Add a series to the DataFrame"},
    {"shape", (PyCFunction) DataFrame_shape, METH_NOARGS,
     "Get the shape of the DataFrame"},
    {NULL}  /* Sentinel */
};

static PyTypeObject DataFrameType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "gopolars._gopolars.DataFrame",
    .tp_doc = "DataFrame object",
    .tp_basicsize = sizeof(DataFrameObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = DataFrame_new,
    .tp_dealloc = (destructor) DataFrame_dealloc,
    .tp_methods = DataFrame_methods,
};

static PyModuleDef gopolarsmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "gopolars._gopolars",
    .m_doc = "Python interface for go-polars.",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit__gopolars(void)
{
    import_array();  // Initialize NumPy

    PyObject *m;
    if (PyType_Ready(&DataFrameType) < 0)
        return NULL;

    m = PyModule_Create(&gopolarsmodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&DataFrameType);
    if (PyModule_AddObject(m, "DataFrame", (PyObject *) &DataFrameType) < 0) {
        Py_DECREF(&DataFrameType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
} 