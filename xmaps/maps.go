// Package xmaps provides map functions that extend the standard maps package.
package xmaps

import (
	"errors"
	"fmt"
)

// ErrDuplicateValue is returned, wrapped, by Invert when two keys of its input share a value.
var ErrDuplicateValue = errors.New("xmaps: map has duplicate values")

// Keys returns a new slice of the keys of source.
// It is like slices.Collect(maps.Keys(source)), but allocates the result at len(source) up front.
//
// Important: the order of the result is unspecified, so sort it if a stable order matters.
func Keys[M ~map[K]V, K comparable, V any](source M) []K {
	keys := make([]K, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	return keys
}

// Values returns a new slice of the values of source.
// It is like slices.Collect(maps.Values(source)), but allocates the result at len(source) up front.
//
// Important: the order of the result is unspecified, so sort it if a stable order matters.
func Values[M ~map[K]V, K comparable, V any](source M) []V {
	values := make([]V, 0, len(source))
	for _, value := range source {
		values = append(values, value)
	}
	return values
}

// Invert returns a new map with the keys and values of source swapped.
// If two keys share a value, it returns a nil map and an error that wraps ErrDuplicateValue.
// Which duplicated value is reported depends on map iteration order.
// The standard library has no equivalent.
func Invert[M ~map[K]V, K comparable, V comparable](source M) (map[V]K, error) {
	inverted := make(map[V]K, len(source))
	for key, value := range source {
		if _, ok := inverted[value]; ok {
			return nil, fmt.Errorf("invert: value %v: %w", value, ErrDuplicateValue)
		}
		inverted[value] = key
	}
	return inverted, nil
}

// Merge returns a new map with the entries of all sources.
// On duplicate keys, later maps win.
// Unlike maps.Copy, it does not copy into an existing map.
func Merge[M ~map[K]V, K comparable, V any](sources ...M) M {
	merged := make(M)
	for _, source := range sources {
		for key, value := range source {
			merged[key] = value
		}
	}
	return merged
}
