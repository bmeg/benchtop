//#define Py_LIMITED_API
#define PY_SSIZE_T_CLEAN

// I leave this here to comment out the code. cgo seems not to recompile
// pybenchtop.h unless pybenchtop.c compiles correctly. So I set this to 
// 0 and recompile to get an updated header file. 
#if 1

#include <Python.h>
#include "structmember.h"
#include "pybenchtop.h"


//Header stuff

typedef struct {
    PyObject_HEAD
    //driver here
    uintptr_t driver;
} Driver;

typedef struct {
    PyObject_HEAD
    //table here
    uintptr_t table;
} Table;

static PyTypeObject TableType;
static int Table_init(Table *self, PyObject *args, PyObject *kwds);

// Benchtop Driver class

static PyObject * Driver_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    Driver *self;
    self = (Driver *)type->tp_alloc(type, 0);
    self->driver = 0;
    return (PyObject *)self;
}

static void Driver_dealloc(Driver* self){
    if (self->driver != 0) {
        DriverClose(self->driver);
    }
    self->driver = 0;
    //self->ob_type->tp_free((PyObject*)self);
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

    //TODO: should we release this table?
    uintptr_t table = NewTable(self->driver, tableName, columnDef);

    PyObject *argList = Py_BuildValue("(Os)", self, tableName);
    printf("Calling Object!\n");
    //PyObject *obj = PyObject_CallObject(&TableType, argList);

    PyObject *obj = PyObject_New(Table, &TableType);
    if (Table_init(obj, argList, NULL) != 0) {
        printf("table init error\n");
    }

    Py_DECREF(argList);
    printf("Returning objct\n");
    return obj;
}


static PyObject * Driver_gettable(Driver *self, PyObject *args, PyObject *kwds) {
    char *tableName;
    if (! PyArg_ParseTuple(args, "s", &tableName))
        return NULL;
    PyObject *argList = Py_BuildValue("(Os)", self, tableName);
    PyObject *obj = PyObject_New(Table, &TableType);
    if (Table_init(obj, argList, NULL) != 0) {
        printf("table init error\n");
    }
    Py_DECREF(argList);
    return obj;
}

static PyObject * Driver_close(Driver *self, PyObject *args, PyObject *kwds) {
    if (self->driver != 0) {
        DriverClose(self->driver);
    }
    self->driver = 0;
    Py_RETURN_NONE;
}

static PyMemberDef Driver_members[] = {
    {NULL}  /* Sentinel */
};

static PyMethodDef Driver_methods[] = {
    {"new", (PyCFunction)Driver_newtable, METH_VARARGS, "Generate a new table",},
    {"get", (PyCFunction)Driver_gettable, METH_VARARGS, "Get an existing table",},
    {"close", (PyCFunction)Driver_close, METH_VARARGS, "Close database",},
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



static PyObject * Table_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    printf("Calling table new\n");
    Table *self;
    self = (Table *)type->tp_alloc(type, 0);
    self->table = 0;
    return (PyObject *)self;
}

static void Table_dealloc(Table* self){
    if (self->table != 0) {
        CloseTable(self->table);
    }
    //self->ob_type->tp_free((PyObject*)self);
}

static int Table_init(Table *self, PyObject *args, PyObject *kwds) {
    printf("Calling table init\n");
    char *name;
    PyObject *pyObj;

    if (! PyArg_ParseTuple(args, "Os", &pyObj, &name))
        return -1;

    //check pyobject to ensure it is a driver
    Driver *dr = (Driver *)pyObj;

    uintptr_t tb = GetTable(dr->driver, name);
    if (tb == 0) {
        printf("Table not found\n");
        PyErr_SetString(PyExc_TypeError, "table not found");
        return -1;  
    }
    printf("Returning Table\n");
    self->table = tb;
    return 0;
}

static PyObject * Table_add(Table *self, PyObject *args, PyObject *kwds) {
    char *key;
    PyObject *data;

    if (! PyArg_ParseTuple(args, "sO", &key, &data))
       Py_RETURN_NONE;

    AddDataTable(self->table, key, data);
    return PyUnicode_FromFormat("Running table add");
}

static PyObject * Table_get(Table *self, PyObject *args, PyObject *kwds) {
    char *key;
    
    if (! PyArg_ParseTuple(args, "s", &key))
        Py_RETURN_NONE;

    PyObject *data = GetDataTable(self->table, key);
    if (data == NULL) {
        PyErr_SetString(PyExc_TypeError, "data not found");
        return NULL;
    }    
    return data;
}

static PyMemberDef Table_members[] = {
    {NULL}  /* Sentinel */
};

static PyMethodDef Table_methods[] = {
    {"add", (PyCFunction)Table_add, METH_VARARGS, "Add data to table",},
    {"get", (PyCFunction)Table_get, METH_VARARGS, "Get data from table",},
    {NULL}  /* Sentinel */
};


static PyTypeObject TableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pybenchtop.Table",
    .tp_doc = "Custom objects",
    .tp_basicsize = sizeof(Table),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE ,
    .tp_new = Table_new,
    .tp_init = (initproc) Table_init,
    .tp_dealloc = (destructor) Table_dealloc,
    .tp_members = Table_members,
    .tp_methods = Table_methods,
};


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