package main

// #cgo pkg-config: python3
// #cgo LDFLAGS: -lpython3.12
// #define Py_LIMITED_API
// #include <Python.h>
// #include <stdint.h> // for uintptr_t
// #include "shim.h"
// int PyArg_ParseTuple_LL(PyObject *, long long *, long long *);
import "C"

import (
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/bmeg/benchtop"
)

//export NewDriver
func NewDriver(base *C.char) uintptr {
	fmt.Printf("Creating a driver\n")
	s := C.GoString(base)
	o, err := benchtop.NewBSONDriver(s)
	if err != nil {
		//TODO: clean this up
		fmt.Printf("Error!!!: %s\n", err)
	}
	out := uintptr(cgo.NewHandle(o))
	return out
}

//export DriverClose
func DriverClose(d uintptr) {
	fmt.Printf("Calling db close\n")
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)
	dr.Close()
}

//export NewTable
func NewTable(d uintptr, name *C.char, def *C.PyObject) uintptr {

	nameField := C.CString("__name__")
	defer C.free(unsafe.Pointer(nameField))

	gname := C.GoString(name)
	fmt.Printf("Building Table: %s\n", gname)
	cdef := []benchtop.ColumnDef{}
	if C._go_PyDict_Check(def) != 0 {
		items := C.PyDict_Items(def)
		itemCount := C.PyList_Size(items)
		fmt.Printf("Dict with items: %#v (%d)\n", items, itemCount)
		for i := 0; i < int(itemCount); i++ {
			it := C.PyList_GetItem(items, C.Py_ssize_t(i))
			fmt.Printf("\tItem %#v\n", it)
			key := C.PyTuple_GetItem(it, 0)
			var keyBytes *C.char = C._go_PyStr_AsString(key)
			keyStr := C.GoString(keyBytes)
			fmt.Printf("Key: %s\n", keyStr)

			value := C.PyTuple_GetItem(it, 1)
			if C.PyType_Check(value) != 0 {
				// typeName := C.PyType_GetName(value) // added in 3.12
				valueName := C.PyObject_GetAttrString(value, nameField)
				valueNameCStr := C._go_PyStr_AsString((*C.PyObject)(valueName))
				valueNameStr := C.GoString(valueNameCStr)
				if valueNameStr == "float" {
					fmt.Printf("Type float\n")
					cdef = append(cdef, benchtop.ColumnDef{Path: keyStr, Type: benchtop.Double})
				} else {
					fmt.Printf("Type Value: %s\n", valueNameStr)
				}
			}
		}
	}
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)

	table := dr.New(gname, cdef)
	out := uintptr(cgo.NewHandle(table))
	return out
}

//export HelloWorld
func HelloWorld() {
	fmt.Printf("Hello world!!\n")
}

func main() {}
