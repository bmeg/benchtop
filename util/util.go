package util

import (
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
