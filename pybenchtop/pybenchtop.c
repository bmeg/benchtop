//#define Py_LIMITED_API
#define PY_SSIZE_T_CLEAN

// I leave this here to comment out the code. cgo seems not to recompile
// pybenchtop.h unless pybenchtop.c compiles correctly. So I set this to 
// 0 and recompile to get an updated header file. 
#if 1

#include <Python.h>
#include "structmember.h"
#include "pybenchtop.h"

// Benchtop Driver class

typedef struct {
    PyObject_HEAD
    //driver here
    uintptr_t driver;
} Driver;


static void Driver_dealloc(Driver* self){
    if (self->driver != 0) {
        DriverClose(self->driver);
    }
    //self->ob_type->tp_free((PyObject*)self);
}

static PyObject * Driver_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    Driver *self;
    self = (Driver *)type->tp_alloc(type, 0);
    self->driver = 0;
    return (PyObject *)self;
}

static int Driver_init(Driver *self, PyObject *args, PyObject *kwds) {
    char *base;
    if (! PyArg_ParseTuple(args, "s", &base))
        return -1;

    uintptr_t dr = NewDriver(base) ;
    self->driver = dr;
    return 0;
}

static PyObject * Driver_newtable(Driver *self, PyObject *args, PyObject *kwds) {
    char *tableName;
    PyObject *columnDef;
    if (! PyArg_ParseTuple(args, "sO", &tableName, &columnDef))
        return NULL;

    printf("Adding table: %s\n", tableName);

    uintptr_t table = NewTable(self->driver, tableName, columnDef);

    return PyUnicode_FromFormat("Cool test bro");
}


static PyMemberDef Driver_members[] = {
    {NULL}  /* Sentinel */
};

static PyMethodDef Driver_methods[] = {
    {"new", (PyCFunction)Driver_newtable, METH_VARARGS, "Generate a new table",},
    {NULL}  /* Sentinel */
};


static PyTypeObject DriverType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pybenchtop.Driver",
    .tp_doc = "Custom objects",
    .tp_basicsize = sizeof(Driver),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE ,
    .tp_new = Driver_new,
    .tp_init = (initproc) Driver_init,
    .tp_dealloc = (destructor) Driver_dealloc,
    .tp_members = Driver_members,
    .tp_methods = Driver_methods,
};

// Table interface


static PyObject * hello_world(PyObject *self, PyObject *args) {
    HelloWorld();
    return PyLong_FromLongLong(0);
}

// Add methods to the class here
static PyMethodDef BenchMethods[] = {
    {NULL, NULL, 0, NULL}        // Sentinel
};

static struct PyModuleDef btmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "benchtop",   // name of module
    .m_size = -1,
    .m_methods = BenchMethods
};

PyMODINIT_FUNC
PyInit_pybenchtop(void) {
    PyObject *m = PyModule_Create(&btmodule);

    if (PyType_Ready(&DriverType) < 0)
        return NULL;

    Py_INCREF(&DriverType);
    PyModule_AddObject(m, "Driver", (PyObject *)&DriverType);

    return m;
}

#endif