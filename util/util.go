package util

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"golang.org/x/exp/rand"
)

// RandomString generates a random string of length n.
func RandomString(n int) string {
	r := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[r.Intn(len(letter))]
	}
	return string(b)
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return err != os.ErrNotExist
}

func DirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func CopyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func PadToSixDigits(number int) string {
	return fmt.Sprintf("%06d", number)
}

func SliceToAny(v any) []any {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return []any{v}
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
}
