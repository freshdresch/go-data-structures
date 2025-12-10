// package slices provides convenience functions around slices missing from the Go standard library
package slices

// Unique returns a slice containing the first occurrence of each element from inSlice,
// preserving the original order.
func Unique[T comparable](inSlice []T) []T {
	seen := make(map[T]struct{})
	unique := make([]T, 0)
	for _, item := range inSlice {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		unique = append(unique, item)
	}
	return unique
}

// Reverse returns a new slice containing the elements of inSlice in reverse order.
// The input slice is not modified; the returned slice has the same length as inSlice
// (an empty slice when inSlice is nil or empty).
func Reverse[T any](inSlice []T) []T {
	reversed := make([]T, len(inSlice))
	fwdItr := 0
	for backItr := len(inSlice) - 1; backItr >= 0; backItr-- {
		reversed[fwdItr] = inSlice[backItr]
		fwdItr++
	}
	return reversed
}

// IndexOf returns the index of the first occurrence of elem in inSlice.
// It returns the index, or -1 if elem is not present.
func IndexOf[T comparable](inSlice []T, elem T) int {
	for i, item := range inSlice {
		if item == elem {
			return i
		}
	}
	return -1
}

// LastIndexOf reports the index of the last occurrence of elem in inSlice.
// It returns the index of that occurrence, or -1 if elem is not present.
func LastIndexOf[T comparable](inSlice []T, elem T) int {
	for i := len(inSlice) - 1; i >= 0; i-- {
		if inSlice[i] == elem {
			return i
		}
	}
	return -1
}

// IndicesOf returns the indices of all occurrences of elem in inSlice in ascending order.
// If elem is not present, it returns an empty slice.
func IndicesOf[T comparable](inSlice []T, elem T) []int {
	indices := []int{}
	for i, item := range inSlice {
		if item == elem {
			indices = append(indices, i)
		}
	}
	return indices
}

// Contains reports whether the slice contains the specified element.
// It returns true if an element equal to elem is present, false otherwise.
func Contains[T comparable](inSlice []T, elem T) bool {
	return IndexOf(inSlice, elem) != -1
}

// Partition splits a slice into two slices according to the provided predicate.
// The first returned slice contains elements for which the predicate returns true;
// the second contains the remaining elements. Element order from the input is preserved.
func Partition[T any](inSlice []T, predicate func(T) bool) ([]T, []T) {
	trueSlice := make([]T, 0)
	falseSlice := make([]T, 0)
	for _, item := range inSlice {
		if predicate(item) {
			trueSlice = append(trueSlice, item)
		} else {
			falseSlice = append(falseSlice, item)
		}
	}
	return trueSlice, falseSlice
}