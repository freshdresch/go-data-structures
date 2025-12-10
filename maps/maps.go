// package maps provides convenience functions around maps missing from the Go standard library
package maps

import "errors"

// Keys returns a slice of keys from a given map.
//
// Important: the slice will be in a non-deterministic order due to the properties of iterating
// Keys extracts all keys from m into a slice.
// The iteration order is not specified; if a stable order is required the caller should sort the result.
// If m is nil the function returns an empty slice.
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
// Values returns a slice containing all values present in m.
// The iteration order over a map is not guaranteed, so the resulting slice has a non-deterministic order;
// the caller should sort the slice if a stable order is required.
func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, val := range m {
		values = append(values, val)
	}
	return values
}

// Invert returns a new map with keys and values swapped from the input map.
// It produces an error if the input map contains duplicate values (which would
// collide when used as keys in the inverted map).
//
// If successful, the returned map maps each distinct value from the input to
// its corresponding key.
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

// Merge merges the provided maps into a new map, with entries from later maps
// overwriting entries from earlier ones when keys collide.
// If no maps are provided an empty map is returned.
func Merge[K comparable, V any](maps ...map[K]V) map[K]V {
	merged := make(map[K]V)
	for _, m := range maps {
		for key, val := range m {
			merged[key] = val
		}
	}
	return merged
}