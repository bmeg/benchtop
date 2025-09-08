package util

import (
	"os"
	"strconv"
	"strings"
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
	numStr := strconv.Itoa(number)
	numZeros := 6 - len(numStr)
	if numZeros < 0 {
		return numStr
	}
	return strings.Repeat("0", numZeros) + numStr
}
