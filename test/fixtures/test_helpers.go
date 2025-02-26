package fixtures

import (
	"crypto/rand"
	"math/big"
)

const (
	NumKeys   = 1000000
	ValueSize = 5024
)

func GenerateRandomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func GetRandomUniqueIntegers(count, max int) (map[int]struct{}, error) {
	uniqueNumbers := make(map[int]struct{})
	for len(uniqueNumbers) < count {
		nBig, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
		if err != nil {
			return nil, err
		}
		num := int(nBig.Int64())

		if _, exists := uniqueNumbers[num]; !exists {
			uniqueNumbers[num] = struct{}{}
		}
	}

	return uniqueNumbers, nil
}

var Basicdata = map[string]map[string]any{
	"key1": {
		"field1": 0.1,
		"name":   "alice",
		"other":  "other data",
	},
	"key2": {
		"field1": 0.2,
		"name":   "bob",
	},
	"key3": {
		"field1": 0.3,
		"name":   "chelsie",
	},
}

var ScanData = map[string]map[string]any{
	"key1": {
		"field1": 0.1,
		"name":   "alice",
		"other":  "other data",
		"isEven": true,
	},
	"key2": {
		"field1": 0.2,
		"name":   "bob",
	},
	"key3": {
		"field1": 0.3,
		"name":   "chelsie",
	},
	"key4": {
		"field1": 0.4,
		"name":   "asfg",
	},
	"key5": {
		"field1": 0.5,
		"name":   "hgfd",
	},
	"key6": {
		"field1": 0.6,
		"name":   "qwer",
	},
	"key7": {
		"field1": 0.7,
		"name":   "zxcv",
	},
	"key8": {
		"field1": 0.8,
		"name":   "mnbv",
	},
}
