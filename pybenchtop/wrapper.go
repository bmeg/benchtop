package main

// #cgo pkg-config: python3-embed
// #define Py_LIMITED_API
// #include <Python.h>
// #include <stdint.h> // for uintptr_t
// #include "shim.h"
import "C"

import (
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/bmeg/benchtop"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
			var keyBytes *C.char = C._go_PyUnicode_AsUTF8(key)
			keyStr := C.GoString(keyBytes)
			fmt.Printf("Key: %s\n", keyStr)

			value := C.PyTuple_GetItem(it, 1)
			if C._go_PyType_Check(value) != 0 {
				// typeName := C.PyType_GetName(value) // added in 3.12
				valueName := C.PyObject_GetAttrString(value, nameField)
				valueNameCStr := C._go_PyUnicode_AsUTF8((*C.PyObject)(valueName))
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
		fmt.Printf("Error: %s\n", err)
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
func GetDataTable(tb uintptr, name *C.char) *C.PyObject {
	table := cgo.Handle(tb).Value().(benchtop.TableStore)
	data, err := table.Get([]byte(C.GoString(name)))
	if err != nil {
		return nil
	}
	return Go2PyObject(data)
}

func PyDict2Go(obj *C.PyObject) map[string]any {
	out := map[string]any{}
	items := C.PyDict_Items(obj)
	itemCount := C.PyList_Size(items)
	for i := 0; i < int(itemCount); i++ {
		it := C.PyList_GetItem(items, C.Py_ssize_t(i))
		key := C.PyTuple_GetItem(it, 0)
		var keyBytes *C.char = C._go_PyUnicode_AsUTF8(key)
		keyStr := C.GoString(keyBytes)
		value := C.PyTuple_GetItem(it, 1)
		out[keyStr] = PyObject2Go(value)
	}
	return out
}

func PyList2Go(obj *C.PyObject) []any {
	out := []any{}
	for i := 0; i < int(C.PyList_Size(obj)); i++ {
		item := C._go_PyList_GetItem(obj, C.int(i))
		out = append(out, PyObject2Go(item))
	}
	return out
}

func PyObject2Go(obj *C.PyObject) any {
	if C._go_PyDict_Check(obj) != 0 {
		return PyDict2Go(obj)
	} else if C._go_PyList_Check(obj) != 0 {
		return PyList2Go(obj)
	} else if C._go_PyUnicode_Check(obj) != 0 {
		s := C._go_PyUnicode_AsUTF8(obj)
		return C.GoString(s)
	} else if C._go_PyFloat_Check(obj) != 0 {
		return C.PyFloat_AsDouble(obj)
	} else if C._go_PyLong_Check(obj) != 0 {
		return C.PyLong_AsLong(obj)
	} //TODO: other types
	return nil
}

func Go2PyObject(data any) *C.PyObject {

	switch value := data.(type) {
	case map[string]any:
		out := C.PyDict_New()
		for k, v := range value {
			vObj := Go2PyObject(v)
			C.PyDict_SetItemString(out, C.CString(k), vObj)
		}
		return out
	case []any:
		out := C.PyList_New(0)
		for _, v := range value {
			vObj := Go2PyObject(v)
			C.PyList_Append(out, vObj)
		}
		return out
	case primitive.A:
		out := C.PyList_New(0)
		for _, v := range value {
			vObj := Go2PyObject(v)
			C.PyList_Append(out, vObj)
		}
		return out
	case int64:
		return C.PyLong_FromLong(C.long(int64(value)))
	case int32:
		return C.PyLong_FromLong(C.long(int64(value)))
	case float32:
		return C.PyFloat_FromDouble(C.double(float64(value)))
	case float64:
		return C.PyFloat_FromDouble(C.double(float64(value)))
	case string:
		return C.PyUnicode_FromString(C.CString(value))
	default:
		fmt.Printf("Unknown type: %#v\n", value)
	}
	return C.Py_None
}

func main() {}
