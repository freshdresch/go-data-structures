// package maps provides convenience functions around maps missing from the Go standard library
package maps

import "errors"

// Keys returns a slice of keys from a given map.
//
// Important: the slice will be in a non-deterministic order due to the properties of iterating
// through a Go map. The caller should sort the slice on their own if a stable order is necessary.
func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// Values returns a slice of values from a given map.
//
// Important: the slice will be in a non-deterministic order due to the properties of iterating
// through a Go map. The caller should sort the slice on their own if a stable order is necessary.
func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, val := range m {
		values = append(values, val)
	}
	return values
}

// Invert inverts a map, swapping its keys and values.
func Invert[K comparable, V comparable](m map[K]V) (map[V]K, error) {
	inverted := make(map[V]K)
	for key, val := range m {
		if _, ok := inverted[val]; ok {
			return nil, errors.New("map has non-unique values")
		}
		inverted[val] = key
	}
	return inverted, nil
}

// Merge merges two or more maps into a single map.
func Merge[K comparable, V any](maps ...map[K]V) map[K]V {
	merged := make(map[K]V)
	for _, m := range maps {
		for key, val := range m {
			merged[key] = val
		}
	}
	return merged
}
