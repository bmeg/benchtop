package tpath

import (
	"strings"

	"github.com/bmeg/jsonpath"
)

// Current represents the 'current' traveler namespace
const CURRENT = "_current"

func PathLookup(v map[string]any, path string) any {
	/* Expects that special fields like '_id' and '_label'
	   are added to the map before reaching this function
	*/
	field := NormalizePath(path)
	jpath := ToLocalPath(field)
	res, err := jsonpath.JsonPathLookup(v, jpath)
	if err != nil {
		return nil
	}
	return res
}

// GetNamespace returns the namespace of the provided path
//
// Example:
// GetNamespace("$gene.symbol.ensembl") returns "gene"
func GetNamespace(path string) string {
	namespace := ""
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		namespace = strings.TrimPrefix(parts[0], "$")
	}
	if namespace == "" {
		namespace = CURRENT
	}
	return namespace
}

// NormalizePath
//
// Example:
// NormalizePath("gene.symbol.ensembl") returns "$_current.symbol.ensembl"

func NormalizePath(path string) string {
	namespace := CURRENT
	parts := strings.Split(path, ".")

	if strings.HasPrefix(parts[0], "$") {
		if len(parts[0]) > 1 {
			namespace = parts[0][1:]
		}
		parts = parts[1:]
	}

	parts = append([]string{"$" + namespace}, parts...)
	return strings.Join(parts, ".")
}

func ToLocalPath(path string) string {
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		parts[0] = "$"
	}
	return strings.Join(parts, ".")
}
