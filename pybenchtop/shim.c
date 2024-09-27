
#include <Python.h>

// I have no idea why this is needed, but it works.
// Trying to call it directly gets the error: 'could not determine kind of name for C.PyDict_Check'
int _go_PyDict_Check(PyObject *p) {
    return PyDict_Check(p);
}

char * _go_PyStr_AsString(PyObject *p) {
    return PyUnicode_AsUTF8(p);
}