// Package xslices provides slice functions that extend the standard slices package.
package xslices

import "slices"

// Unique returns a new slice of the unique elements of inSlice, in first-seen order.
// Unlike slices.Compact, it removes all duplicates, not only adjacent ones.
// The result has the same type as inSlice.
func Unique[S ~[]E, E comparable](inSlice S) S {
	seen := make(map[E]struct{})
	unique := make(S, 0)
	for _, item := range inSlice {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		unique = append(unique, item)
	}
	return unique
}

// Reverse reverses the order of the elements of inSlice in place.
// It is the same as slices.Reverse and calls it.
func Reverse[S ~[]E, E any](inSlice S) {
	slices.Reverse(inSlice)
}

// Reversed returns a new slice with the elements of inSlice in reverse order.
// Unlike the in-place slices.Reverse, it leaves inSlice unchanged.
// The result has the same type as inSlice.
func Reversed[S ~[]E, E any](inSlice S) S {
	reversed := make(S, len(inSlice))
	for i, item := range inSlice {
		reversed[len(inSlice)-1-i] = item
	}
	return reversed
}

// IndexOf returns the index of the first occurrence of element in inSlice, or -1 if it is absent.
// It is the same as slices.Index and calls it.
func IndexOf[S ~[]E, E comparable](inSlice S, element E) int {
	return slices.Index(inSlice, element)
}

// LastIndexOf returns the index of the last occurrence of element in inSlice, or -1 if it is
// absent. The standard library has no equivalent.
func LastIndexOf[S ~[]E, E comparable](inSlice S, element E) int {
	for i := len(inSlice) - 1; i >= 0; i-- {
		if inSlice[i] == element {
			return i
		}
	}
	return -1
}

// IndicesOf returns the indices of every occurrence of element in inSlice, in increasing order.
// The standard library has no equivalent.
func IndicesOf[S ~[]E, E comparable](inSlice S, element E) []int {
	indices := []int{}
	for i, item := range inSlice {
		if item == element {
			indices = append(indices, i)
		}
	}
	return indices
}

// Contains reports whether element is in inSlice.
// It is the same as slices.Contains and calls it.
func Contains[S ~[]E, E comparable](inSlice S, element E) bool {
	return slices.Contains(inSlice, element)
}

// Partition splits inSlice into the elements that satisfy predicate and the elements that do
// not, each in its original order. The standard library has no equivalent.
// Both results have the same type as inSlice.
func Partition[S ~[]E, E any](inSlice S, predicate func(E) bool) (S, S) {
	trueSlice := make(S, 0)
	falseSlice := make(S, 0)
	for _, item := range inSlice {
		if predicate(item) {
			trueSlice = append(trueSlice, item)
		} else {
			falseSlice = append(falseSlice, item)
		}
	}
	return trueSlice, falseSlice
}
