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

	table, err := dr.New(gname, cdef)
	if err != nil {
		return 0
	}
	out := uintptr(cgo.NewHandle(table))
	return out
}

//export GetTable
func GetTable(d uintptr, name *C.char) uintptr {
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)
	table, err := dr.Get(C.GoString(name))
	if err != nil {
		return 0
	}
	return uintptr(cgo.NewHandle(table))
}

//export CloseTable
func CloseTable(tb uintptr) {
	table := cgo.Handle(tb).Value().(benchtop.TableStore)
	table.Close()
}

//export AddDataTable
func AddDataTable(tb uintptr, name *C.char, obj *C.PyObject) {
	data := PyDict2Go(obj)
	table := cgo.Handle(tb).Value().(benchtop.TableStore)
	table.Add([]byte(C.GoString(name)), data)
}

//export GetDataTable
func GetDataTable(t uintptr, name *C.char) *C.PyObject {
	return nil
}

func PyDict2Go(obj *C.PyObject) map[string]any {
	out := map[string]any{}
	items := C.PyDict_Items(obj)
	itemCount := C.PyList_Size(items)
	for i := 0; i < int(itemCount); i++ {
		it := C.PyList_GetItem(items, C.Py_ssize_t(i))
		key := C.PyTuple_GetItem(it, 0)
		var keyBytes *C.char = C._go_PyStr_AsString(key)
		keyStr := C.GoString(keyBytes)
		value := C.PyTuple_GetItem(it, 1)
		out[keyStr] = PyObject2Go(value)
	}
	return out
}

func PyObject2Go(obj *C.PyObject) any {
	if C._go_PyDict_Check(obj) != 0 {
		return PyDict2Go(obj)
	} //TODO: other types
	return nil
}

func main() {}
