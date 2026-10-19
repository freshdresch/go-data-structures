package xslices

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type names []string

func TestUnique(t *testing.T) {
	letters := []string{"a", "b", "c", "a", "b", "c", "d"}
	expected := []string{"a", "b", "c", "d"}
	assert.Equal(t, expected, Unique(letters))
}

func TestUniqueNamedType(t *testing.T) {
	input := names{"a", "b", "a"}
	assert.Equal(t, names{"a", "b"}, Unique(input))
}

func TestReverse(t *testing.T) {
	letters := []string{"a", "b", "c", "a", "b", "c", "d"}
	Reverse(letters)
	expected := []string{"d", "c", "b", "a", "c", "b", "a"}
	assert.Equal(t, expected, letters)
}

func TestReverseNamedType(t *testing.T) {
	input := names{"a", "b", "c"}
	Reverse(input)
	assert.Equal(t, names{"c", "b", "a"}, input)
}

func TestReversed(t *testing.T) {
	letters := []string{"a", "b", "c", "a", "b", "c", "d"}
	expected := []string{"d", "c", "b", "a", "c", "b", "a"}
	assert.Equal(t, expected, Reversed(letters))
	assert.Equal(t, []string{"a", "b", "c", "a", "b", "c", "d"}, letters)
}

func TestReversedNamedType(t *testing.T) {
	input := names{"a", "b", "c"}
	assert.Equal(t, names{"c", "b", "a"}, Reversed(input))
}

func TestIndexing(t *testing.T) {
	letters := []string{"a", "b", "c", "a", "b", "c", "d"}
	assert.Equal(t, 0, IndexOf(letters, "a"))
	assert.Equal(t, 3, LastIndexOf(letters, "a"))
	assert.Equal(t, -1, LastIndexOf(letters, "e"))
	assert.Equal(t, []int{0, 3}, IndicesOf(letters, "a"))
}

func TestIndexOfNamedType(t *testing.T) {
	input := names{"a", "b", "a"}
	assert.Equal(t, 1, IndexOf(input, "b"))
}

func TestLastIndexOfNamedType(t *testing.T) {
	input := names{"a", "b", "a"}
	assert.Equal(t, 2, LastIndexOf(input, "a"))
}

func TestIndicesOfNamedType(t *testing.T) {
	input := names{"a", "b", "a"}
	assert.Equal(t, []int{0, 2}, IndicesOf(input, "a"))
}

func TestContains(t *testing.T) {
	letters := []string{"a", "b", "c", "a", "b", "c", "d"}
	assert.True(t, Contains(letters, "a"))
	assert.True(t, Contains(letters, "b"))
	assert.True(t, Contains(letters, "c"))
	assert.True(t, Contains(letters, "d"))
	assert.False(t, Contains(letters, "e"))
	assert.False(t, Contains(letters, "f"))
	assert.False(t, Contains(letters, "g"))
}

func TestContainsNamedType(t *testing.T) {
	input := names{"a", "b"}
	assert.True(t, Contains(input, "b"))
	assert.False(t, Contains(input, "c"))
}

func TestPartition(t *testing.T) {
	numbers := []int{1, 2, 3, 4, 5, 6}
	isEven := func(number int) bool { return number%2 == 0 }
	even, odd := Partition(numbers, isEven)
	assert.Equal(t, []int{2, 4, 6}, even)
	assert.Equal(t, []int{1, 3, 5}, odd)
}

func TestPartitionNamedType(t *testing.T) {
	input := names{"apple", "kiwi", "banana"}
	isLong := func(name string) bool { return len(name) > 4 }
	long, short := Partition(input, isLong)
	assert.Equal(t, names{"apple", "banana"}, long)
	assert.Equal(t, names{"kiwi"}, short)
}

func TestNilInputs(t *testing.T) {
	assert.NotNil(t, Unique([]int(nil)))
	assert.Empty(t, Unique([]int(nil)))
	assert.NotNil(t, Reversed([]int(nil)))
	assert.Empty(t, Reversed([]int(nil)))

	evens, odds := Partition([]int(nil), func(number int) bool { return number%2 == 0 })
	assert.NotNil(t, evens)
	assert.Empty(t, evens)
	assert.NotNil(t, odds)
	assert.Empty(t, odds)

	assert.NotNil(t, IndicesOf([]int(nil), 1))
	assert.Empty(t, IndicesOf([]int(nil), 1))
	assert.Equal(t, -1, IndexOf([]int{1, 2}, 3))
}
