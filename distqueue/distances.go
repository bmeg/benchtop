package distqueue

import "math"

func Euclidean(a []float32, b []float32) float32 {
	s := float32(0.0)
	for i := range a {
		x := a[i] - b[i]
		s += (x * x)
	}
	return float32(math.Sqrt(float64(s)))
}
