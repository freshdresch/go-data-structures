// package slices provides convenience functions around slices missing from the Go standard library
package slices

// Unique returns a slice of unique elements from a given slice.
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

// Reverse reverses the order of elements in a slice.
func Reverse[T any](inSlice []T) []T {
	reversed := make([]T, len(inSlice))
	fwdItr := 0
	for backItr := len(inSlice) - 1; backItr >= 0; backItr-- {
		reversed[fwdItr] = inSlice[backItr]
		fwdItr++
	}
	return reversed
}

// IndexOf returns the index of the first occurrence of a specified element in a slice.
func IndexOf[T comparable](inSlice []T, elem T) int {
	for i, item := range inSlice {
		if item == elem {
			return i
		}
	}
	return -1
}

// LastIndexOf returns the index of the last occurrence of a specified element in a slice.
func LastIndexOf[T comparable](inSlice []T, elem T) int {
	for i := len(inSlice) - 1; i >= 0; i-- {
		if inSlice[i] == elem {
			return i
		}
	}
	return -1
}

// IndicesOf returns a slice of indices of all occurrences of a specified element in a slice.
func IndicesOf[T comparable](inSlice []T, elem T) []int {
	indices := []int{}
	for i, item := range inSlice {
		if item == elem {
			indices = append(indices, i)
		}
	}
	return indices
}

// Contains checks if a slice contains a specified element.
func Contains[T comparable](inSlice []T, elem T) bool {
	return IndexOf(inSlice, elem) != -1
}

// Partition partitions a slice based on a predicate function.
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
