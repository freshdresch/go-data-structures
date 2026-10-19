package xmaps

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type scores map[string]int

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

func TestKeysNamedType(t *testing.T) {
	input := scores{"a": 1}
	assert.Equal(t, []string{"a"}, Keys(input))
}

func TestValues(t *testing.T) {
	actual := Values(testMap)
	sort.Slice(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})

	// Each key contributes its value, so the duplicate "2" appears twice.
	expected := []string{"1", "2", "2", "3"}
	assert.Equal(t, expected, actual)
}

func TestValuesNamedType(t *testing.T) {
	input := scores{"a": 1}
	assert.Equal(t, []int{1}, Values(input))
}

func TestInvert(t *testing.T) {
	failedInvert, err := Invert(testMap)
	assert.ErrorIs(t, err, ErrDuplicateValue)
	assert.Nil(t, failedInvert)

	source := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	expected := map[string]string{
		"1": "a",
		"2": "b",
		"3": "c",
	}

	inverted, err := Invert(source)
	assert.NoError(t, err)
	assert.Equal(t, expected, inverted)
}

func TestInvertNamedType(t *testing.T) {
	input := scores{"a": 1, "b": 2}
	expected := map[int]string{1: "a", 2: "b"}

	inverted, err := Invert(input)
	assert.NoError(t, err)
	assert.Equal(t, expected, inverted)
}

func TestMerge(t *testing.T) {
	firstMap := map[string]string{
		"e": "5",
		"f": "6",
		"g": "7",
	}

	secondMap := map[string]string{
		"h": "3",
		"i": "2",
		"j": "1",
	}

	merged := Merge(testMap, firstMap, secondMap)

	for key, expected := range testMap {
		actual, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, expected, actual)
	}

	for key, expected := range firstMap {
		actual, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, expected, actual)
	}

	for key, expected := range secondMap {
		actual, ok := merged[key]
		assert.True(t, ok)
		assert.Equal(t, expected, actual)
	}
}

func TestMergeNamedType(t *testing.T) {
	first := scores{"a": 1, "b": 2}
	second := scores{"b": 3}
	assert.Equal(t, scores{"a": 1, "b": 3}, Merge(first, second))
}
