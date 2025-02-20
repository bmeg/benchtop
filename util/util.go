package util

import (
	"os"
	"time"

	"golang.org/x/exp/rand"
)

// RandomString generates a random string of length n.
func RandomString(n int) string {
	rand.NewSource(uint64(time.Now().UnixNano()))
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
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

func CopyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
