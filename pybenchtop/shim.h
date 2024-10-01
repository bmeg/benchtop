
#include<Python.h>

int _go_PyType_Check(PyObject *p);
int _go_PyDict_Check(PyObject *p);
int _go_PyUnicode_Check(PyObject *p);
int _go_PyFloat_Check(PyObject *p);
int _go_PyLong_Check(PyObject *p);
int _go_PyList_Check(PyObject *p);



char * _go_PyUnicode_AsUTF8(PyObject *p);
PyObject * _go_PyList_GetItem(PyObject *d, int i);