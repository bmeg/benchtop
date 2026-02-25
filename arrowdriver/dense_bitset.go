package arrowdriver

import "math/bits"

type denseBitset struct {
	words []uint64
}

func newDenseBitset(size int) *denseBitset {
	if size <= 0 {
		return &denseBitset{}
	}
	return &denseBitset{words: make([]uint64, (size+63)>>6)}
}

func (b *denseBitset) set(i uint32) {
	idx := int(i >> 6)
	if idx < 0 || idx >= len(b.words) {
		return
	}
	b.words[idx] |= 1 << (i & 63)
}

func (b *denseBitset) and(other *denseBitset) {
	if b == nil || other == nil {
		return
	}
	n := len(b.words)
	if len(other.words) < n {
		n = len(other.words)
	}
	for i := 0; i < n; i++ {
		b.words[i] &= other.words[i]
	}
	for i := n; i < len(b.words); i++ {
		b.words[i] = 0
	}
}

func (b *denseBitset) any() bool {
	if b == nil {
		return false
	}
	for _, w := range b.words {
		if w != 0 {
			return true
		}
	}
	return false
}

func (b *denseBitset) indices() []uint32 {
	if b == nil {
		return nil
	}
	out := make([]uint32, 0, len(b.words))
	for wi, w := range b.words {
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			out = append(out, uint32((wi<<6)+tz))
			w &= w - 1
		}
	}
	return out
}

