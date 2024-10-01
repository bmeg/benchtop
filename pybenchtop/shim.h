
#include<Python.h>

int _go_PyDict_Check(PyObject *p);
int _go_PyUnicode_Check(PyObject *p);
int _go_PyFloat_Check(PyObject *p);

char * _go_PyUnicode_AsUTF8(PyObject *p);
