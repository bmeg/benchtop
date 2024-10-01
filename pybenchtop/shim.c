
#include <Python.h>

// I have no idea why this is needed, but it works.
// Trying to call it directly gets the error: 'could not determine kind of name for C.PyDict_Check'
int _go_PyDict_Check(PyObject *p) {
    return PyDict_Check(p);
}

int _go_PyType_Check(PyObject *p) {
    return PyType_Check(p);
}

int _go_PyUnicode_Check(PyObject *p) {
    return PyUnicode_Check(p);
}

int _go_PyFloat_Check(PyObject *p) {
    return PyFloat_Check(p);
}

int _go_PyLong_Check(PyObject *p) {
    return PyLong_Check(p);
}

int _go_PyList_Check(PyObject *p) {
    return PyList_Check(p);
}

char * _go_PyUnicode_AsUTF8(PyObject *p) {
    return (char *)PyUnicode_AsUTF8(p);
}

PyObject* _go_PyList_GetItem(PyObject *obj, int i) {
    return PyList_GetItem(obj, i);
}