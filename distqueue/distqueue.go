package distqueue

import (
	"slices"

	"golang.org/x/exp/constraints"
)

type Element[D constraints.Ordered, V any] struct {
	Dist  D
	Value V
}

type DistQueueMinCapped[D constraints.Ordered, V any] []Element[D, V]
type DistQueueMax[D constraints.Ordered, V any] []Element[D, V]
type DistQueueMin[D constraints.Ordered, V any] []Element[D, V]

//

func minSort[D constraints.Ordered, V any](a, b Element[D, V]) int {
	if a.Dist < b.Dist {
		return -1
	} else if a.Dist > b.Dist {
		return 1
	}
	return 0
}

func maxSort[D constraints.Ordered, V any](a, b Element[D, V]) int {
	if a.Dist < b.Dist {
		return 1
	} else if a.Dist > b.Dist {
		return -1
	}
	return 0
}

//

func NewMinCapped[D constraints.Ordered, V any](c int) DistQueueMinCapped[D, V] {
	out := make(DistQueueMinCapped[D, V], 0, c)
	return out
}

func NewMax[D constraints.Ordered, V any]() DistQueueMax[D, V] {
	out := make(DistQueueMax[D, V], 0, 100)
	return out
}

func NewMin[D constraints.Ordered, V any]() DistQueueMin[D, V] {
	out := make(DistQueueMin[D, V], 0, 100)
	return out
}

func (d *DistQueueMax[D, V]) Insert(dist D, v V) {
	*d = append(*d, Element[D, V]{dist, v})
	slices.SortFunc(*d, maxSort)
}

func (d *DistQueueMin[D, V]) Insert(dist D, v V) {
	*d = append(*d, Element[D, V]{dist, v})
	slices.SortFunc(*d, minSort)
}

func (d *DistQueueMinCapped[D, V]) Insert(dist D, v V) {
	if len(*d) == cap(*d) {
		if dist > (*d)[len(*d)-1].Dist {
			return
		}
		(*d) = (*d)[:len((*d))-1]
	}
	*d = append(*d, Element[D, V]{dist, v})
	slices.SortFunc(*d, minSort)
}

func (d *DistQueueMinCapped[D, V]) Max() D {
	return (*d)[len(*d)-1].Dist
}

func (d *DistQueueMinCapped[D, V]) Filled() bool {
	return len(*d) == cap(*d)
}

func (d *DistQueueMin[D, V]) Pop() (D, V) {
	out := (*d)[0]
	(*d) = (*d)[1:]
	return out.Dist, out.Value
}
