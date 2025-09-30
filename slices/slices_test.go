// package slices provides convenience functions around slices missing from the Go standard library
package slices

import (
        "testing"
        "github.com/stretchr/testify/assert"
)

func TestUnique(t *testing.T) {
        s := []string{"a", "b", "c", "a", "b", "c", "d"}
        expected := []string{"a", "b", "c", "d"}
        assert.Equal(t, expected, Unique(s))
}

func TestReverse(t *testing.T) {
        s := []string{"a", "b", "c", "a", "b", "c", "d"}
        expected := []string{"d", "c", "b", "a", "c", "b", "a"}
        assert.Equal(t, expected, Reverse(s))
}

func TestIndexing(t *testing.T) {
        s := []string{"a", "b", "c", "a", "b", "c", "d"}
        assert.Equal(t, 0, IndexOf(s, "a"))
        assert.Equal(t, 3, LastIndexOf(s, "a"))
        assert.Equal(t, []int{0, 3}, IndicesOf(s, "a"))
}

func TestContains(t *testing.T) {
        s := []string{"a", "b", "c", "a", "b", "c", "d"}
        assert.True(t, Contains(s, "a"))
        assert.True(t, Contains(s, "b"))
        assert.True(t, Contains(s, "c"))
        assert.True(t, Contains(s, "d"))
        assert.False(t, Contains(s, "e"))
        assert.False(t, Contains(s, "f"))
        assert.False(t, Contains(s, "g"))
}

func TestPartition(t *testing.T) {
        numbers := []int{1, 2, 3, 4, 5, 6}
        isEven := func(n int) bool { return n % 2 == 0 }
        even, odd := Partition(numbers, isEven)
        assert.Equal(t, []int{2, 4, 6}, even)
        assert.Equal(t, []int{1, 3, 5}, odd)
}
