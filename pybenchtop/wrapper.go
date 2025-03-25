package main

// #cgo pkg-config: python3-embed
// #define Py_LIMITED_API
// #include <Python.h>
// #include <stdint.h>
// #include "shim.h"
import "C"

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime/cgo"
	"unsafe"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//export NewDriver
func NewDriver(base *C.char) C.uintptr_t {
	s := C.GoString(base)
	// Check if the file/directory exists
	if _, err := os.Stat(s); err == nil {
		// File/directory exists, load the driver
		fmt.Printf("Fetching an existing driver: %s\n", s)
		o, err := bsontable.LoadBSONDriver(s)
		if err != nil {
			fmt.Printf("Error loading driver: %s\n", err)
			return 0 // Return 0 or handle the error appropriately
		}
		return C.uintptr_t(cgo.NewHandle(o))
	} else if os.IsNotExist(err) {
		// File/directory does not exist, create a new driver
		fmt.Printf("Creating a new driver: %s\n", s)
		o, err := bsontable.NewBSONDriver(s)
		if err != nil {
			fmt.Printf("Error creating driver: %s\n", err)
			return 0 // Return 0 or handle the error appropriately
		}
		return C.uintptr_t(cgo.NewHandle(o))
	} else {
		// Some other error occurred during os.Stat
		fmt.Printf("Error checking file existence: %s\n", err)
		return 0 // Return 0 or handle the error appropriately
	}
}

//export DriverClose
func DriverClose(d C.uintptr_t) {
	fmt.Printf("Calling db close\n")
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)
	dr.Close()
	cgo.Handle(d).Delete()
}

//export NewTable
func NewTable(d C.uintptr_t, name *C.char, def *C.PyObject) C.uintptr_t {
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
				valueName := C.PyObject_GetAttrString(value, nameField)
				valueNameCStr := C._go_PyUnicode_AsUTF8((*C.PyObject)(valueName))
				valueNameStr := C.GoString(valueNameCStr)
				C.Py_DECREF(valueName)
				if valueNameStr == "float" {
					cdef = append(cdef, benchtop.ColumnDef{Key: keyStr, Type: benchtop.Double})
				} else if valueNameStr == "Vector" {
					cdef = append(cdef, benchtop.ColumnDef{Key: keyStr, Type: benchtop.VectorArray})
				} else {
					fmt.Printf("Type Value: %s\n", valueNameStr)
				}
			}
		}
	}
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)

	table, err := dr.New(gname, cdef)
	fmt.Printf("TABLE: %#v\n", table)

	if err != nil {
		return 0
	}
	table.(*bsontable.BSONTable).VectorField = "vector"
	return C.uintptr_t(cgo.NewHandle(table))
}

//export GetTable
func GetTable(d C.uintptr_t, name *C.char) C.uintptr_t {
	dr := cgo.Handle(d).Value().(benchtop.TableDriver)
	table, err := dr.Get(C.GoString(name))
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return 0
	}
	return C.uintptr_t(cgo.NewHandle(table))
}

//export CloseTable
func CloseTable(tb C.uintptr_t) {
	table := cgo.Handle(tb).Value().(benchtop.TableStore)
	table.Close()
	cgo.Handle(tb).Delete()
}

//export AddDataTable
func AddDataTable(tb C.uintptr_t, name int, obj *C.PyObject) {
	data := PyDict2Go(obj)
	table := cgo.Handle(tb).Value().(benchtop.TableStore)

	key := make([]byte, 8)

	binary.LittleEndian.PutUint64(key, uint64(name))
	err := table.AddRow(benchtop.Row{Id: key, Data: data})
	if err != nil {
		fmt.Printf("AddDataTable: Error adding row: %s\n", err)
	}
}

//export GetDataTable
func GetDataTable(tb C.uintptr_t, name *C.char) *C.PyObject {
	table := cgo.Handle(tb).Value().(benchtop.TableStore)
	data, err := table.GetRow([]byte(C.GoString(name)))
	if err != nil {
		fmt.Printf("GetDataTable: Error retrieving row %s: %s\n", C.GoString(name), err)
		return nil
	}
	return Go2PyObject(data)
}

//export TableVectorSearch
func TableVectorSearch(tb C.uintptr_t, field *C.char, queryVector *C.PyObject, k C.int) *C.PyObject {
	table, ok := cgo.Handle(tb).Value().(*bsontable.BSONTable)
	if !ok {
		fmt.Println("Error: Table is not a *bsontable.BSONTable")
		return nil
	}

	fieldStr := C.GoString(field)
	vecList := PyList2Go(queryVector)
	queryVec := make([]float32, len(vecList))
	for i, v := range vecList {
		if f, ok := v.(float64); ok {
			queryVec[i] = float32(f)
		} else if f, ok := v.(C.double); ok {
			queryVec[i] = float32(f)
		} else {
			fmt.Printf("Unexpected type: %T\n", v)
			return nil
		}
	}
	//fmt.Printf("QUERY: %s INT: %d QUERY VEC: %v\n", fieldStr, k, queryVec)

	results, err := table.VectorSearch(fieldStr, queryVec, int(k))
	if err != nil {
		fmt.Printf("VectorSearch error: %v\n", err)
		return nil
	}

	pyList := C.PyList_New(C.Py_ssize_t(len(results)))
	for i, res := range results {
		pyDict := C.PyDict_New()
		keyStr := fmt.Sprintf("%d", binary.LittleEndian.Uint64(res.Key)) // Convert to string for Python
		C.PyDict_SetItemString(pyDict, C.CString("key"), C.PyUnicode_FromString(C.CString(keyStr)))
		distance := float64(res.Distance)
		C.PyDict_SetItemString(pyDict, C.CString("distance"), C.PyFloat_FromDouble(C.double(distance)))
		vecListPy := C.PyList_New(C.Py_ssize_t(len(res.Vector)))
		for j, v := range res.Vector {
			C.PyList_SetItem(vecListPy, C.Py_ssize_t(j), C.PyFloat_FromDouble(C.double(v)))
		}
		C.PyDict_SetItemString(pyDict, C.CString("vector"), vecListPy)
		dataObj := Go2PyObject(res.Data)
		C.PyDict_SetItemString(pyDict, C.CString("data"), dataObj)
		C.PyList_SetItem(pyList, C.Py_ssize_t(i), pyDict)
	}
	return pyList
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
		list := PyList2Go(obj)
		if len(list) > 0 {
			if _, ok := list[0].(float64); ok {
				out := make([]float32, len(list))
				for i, v := range list {
					out[i] = float32(v.(float64))
				}
				return out
			} else if _, ok := list[0].(C.double); ok {
				out := make([]float32, len(list))
				for i, v := range list {
					out[i] = float32(v.(C.double))
				}
				return out
			}
		}
		return list
	} else if C._go_PyUnicode_Check(obj) != 0 {
		s := C._go_PyUnicode_AsUTF8(obj)
		return C.GoString(s)
	} else if C._go_PyFloat_Check(obj) != 0 {
		return C.PyFloat_AsDouble(obj)
	} else if C._go_PyLong_Check(obj) != 0 {
		return C.PyLong_AsLong(obj)
	}
	return nil
}

func Go2PyObject(data any) *C.PyObject {
	switch value := data.(type) {
	case map[string]any:
		out := C.PyDict_New()
		for k, v := range value {
			vObj := Go2PyObject(v)
			if vObj == nil {
				vObj = C.Py_None
				C.Py_INCREF(vObj)
			}
			C.PyDict_SetItemString(out, C.CString(k), vObj)
			C.Py_DECREF(vObj)
		}
		return out
	case []any:
		out := C.PyList_New(0)
		for _, v := range value {
			vObj := Go2PyObject(v)
			C.PyList_Append(out, vObj)
			C.Py_DECREF(vObj)
		}
		return out
	case []float32:
		out := C.PyList_New(C.Py_ssize_t(len(value)))
		for i, v := range value {
			vObj := C.PyFloat_FromDouble(C.double(v))
			C.PyList_SetItem(out, C.Py_ssize_t(i), vObj)
		}
		return out
	case primitive.A:
		out := C.PyList_New(0)
		for _, v := range value {
			vObj := Go2PyObject(v)
			C.PyList_Append(out, vObj)
			C.Py_DECREF(vObj)
		}
		return out
	case int64:
		return C.PyLong_FromLong(C.long(value))
	case int32:
		return C.PyLong_FromLong(C.long(value))
	case float32:
		return C.PyFloat_FromDouble(C.double(value))
	case float64:
		return C.PyFloat_FromDouble(C.double(value))
	case string:
		return C.PyUnicode_FromString(C.CString(value))
	case nil:
		return C.Py_None
	default:
		fmt.Printf("Unknown type: %#v\n", value)
		return C.Py_None
	}
}

func main() {}
