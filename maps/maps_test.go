// package maps provides convenience functions around maps missing from the Go standard library
package maps

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

var testMap = map[string]string{
	"a": "1",
	"b": "2",
	"c": "3",
	"d": "2",
}

func TestKeys(t *testing.T) {
	actual := Keys(testMap)
	sort.Slice(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})

	expected := []string{"a", "b", "c", "d"}
	assert.Equal(t, expected, actual)
}

func TestValues(t *testing.T) {
	actual := Values(testMap)
	sort.Slice(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})

	// need to respect the stable order
	expected := []string{"1", "2", "2", "3"}
	assert.Equal(t, expected, actual)
}

func TestInvert(t *testing.T) {
	_, err := Invert(testMap)
	assert.ErrorContains(t, err, "map has non-unique values")

	m := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	expected := map[string]string{
		"1": "a",
		"2": "b",
		"3": "c",
	}

	inverted, err := Invert(m)
	assert.NoError(t, err)
	assert.Equal(t, expected, inverted)
}

func TestMerge(t *testing.T) {
	m1 := map[string]string{
		"e": "5",
		"f": "6",
		"g": "7",
	}

	m2 := map[string]string{
		"h": "3",
		"i": "2",
		"j": "1",
	}

	merged := Merge(testMap, m1, m2)

	for key, val := range testMap {
		v, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, val, v)
	}

	for key, val := range m1 {
		v, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, val, v)
	}

	for key, val := range m2 {
		v, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, val, v)
	}
}
