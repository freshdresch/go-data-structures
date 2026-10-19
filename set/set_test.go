package set

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakingEmptySet(t *testing.T) {
	empty := New[string]()
	assert.True(t, empty.Empty())
}

func TestMakingSetFromSlice(t *testing.T) {
	input := []string{
		"one",
		"two",
		"three",
		"three", // The duplicate should not affect the set.
	}
	words := FromSlice(input)
	assert.True(t, words.Contains("one"))
	assert.True(t, words.Contains("two"))
	assert.True(t, words.Contains("three"))
	assert.Equal(t, 3, words.Len())
}

func TestZeroValueSet(t *testing.T) {
	var zero Set[int]
	assert.True(t, zero.Empty())

	zero.Add(1)
	zero.Add(2)
	assert.Equal(t, 2, zero.Len())
	assert.True(t, zero.Contains(1))
	assert.False(t, zero.Contains(3))

	var updated Set[int]
	updated.Update(&zero)
	assert.Equal(t, 2, updated.Len())
	assert.True(t, updated.Contains(2))

	yielded := 0
	for range updated.All() {
		yielded++
	}
	assert.Equal(t, 2, yielded)
}

func TestAddAndRemove(t *testing.T) {
	cars := New[string]()
	cars.Add("porsche")
	cars.Add("jaguar")
	cars.Add("toyota")

	assert.True(t, cars.Contains("porsche"))
	assert.True(t, cars.Contains("jaguar"))
	assert.True(t, cars.Contains("toyota"))
	assert.False(t, cars.Contains("honda"))
	assert.False(t, cars.Contains("ford"))
	assert.False(t, cars.Contains("jeep"))

	// Test the no-op add too.
	length := cars.Len()
	cars.Add("porsche")
	assert.True(t, cars.Contains("porsche"))
	assert.Equal(t, length, cars.Len())

	other := &Set[string]{
		entries: map[string]struct{}{
			"honda": {},
			"ford":  {},
			"jeep":  {},
		},
	}

	cars.Update(other)

	// Test all of the elements, even the ones we did not expect to change, just to make
	// sure no funny business is happening.
	assert.True(t, cars.Contains("porsche"))
	assert.True(t, cars.Contains("jaguar"))
	assert.True(t, cars.Contains("toyota"))
	assert.True(t, cars.Contains("honda"))
	assert.True(t, cars.Contains("ford"))
	assert.True(t, cars.Contains("jeep"))

	cars.Remove("porsche")

	assert.False(t, cars.Contains("porsche"))
	assert.True(t, cars.Contains("jaguar"))
	assert.True(t, cars.Contains("toyota"))
	assert.True(t, cars.Contains("honda"))
	assert.True(t, cars.Contains("ford"))
	assert.True(t, cars.Contains("jeep"))

	// Test the no-op remove too.
	length = cars.Len()
	cars.Remove("fiat")
	assert.Equal(t, length, cars.Len())

	// Add one more element to the Set that we are going to bulk subtract.
	other.Add("jaguar")
	cars.DifferenceUpdate(other)

	assert.False(t, cars.Contains("porsche"))
	assert.False(t, cars.Contains("jaguar"))
	assert.True(t, cars.Contains("toyota"))
	assert.False(t, cars.Contains("honda"))
	assert.False(t, cars.Contains("ford"))
	assert.False(t, cars.Contains("jeep"))

	// Elements that are already present, or absent, do not change the length after a bulk update.
	// Now cars will have `toyota` and `porsche`.
	cars.Add("porsche")
	assert.Equal(t, 2, cars.Len())

	other = &Set[string]{
		entries: map[string]struct{}{
			"honda":   {},
			"porsche": {},
		},
	}
	cars.Update(other)
	assert.Equal(t, 3, cars.Len())

	other = &Set[string]{
		entries: map[string]struct{}{
			"honda":  {},
			"jaguar": {},
		},
	}
	cars.DifferenceUpdate(other)
	assert.Equal(t, 2, cars.Len())
}

func TestAllYieldsEveryElement(t *testing.T) {
	cars := FromSlice([]string{"porsche", "jaguar", "toyota"})

	seen := make(map[string]int)
	for element := range cars.All() {
		seen[element]++
	}

	assert.Equal(t, map[string]int{"porsche": 1, "jaguar": 1, "toyota": 1}, seen)
}

func TestAllStopsWhenLoopBreaks(t *testing.T) {
	numbers := FromSlice([]int{1, 2, 3, 4, 5})

	calls := 0
	numbers.All()(func(int) bool {
		calls++
		return false
	})

	assert.Equal(t, 1, calls)
}

func TestToSliceReturnsEveryElement(t *testing.T) {
	numbers := FromSlice([]int{3, 1, 2, 2})

	elements := numbers.ToSlice()
	slices.Sort(elements)

	assert.Equal(t, []int{1, 2, 3}, elements)
}

func TestCloneSafety(t *testing.T) {
	cars := &Set[string]{
		entries: map[string]struct{}{
			"honda": {},
			"ford":  {},
			"jeep":  {},
		},
	}

	other := cars.Clone()
	assert.True(t, cars.Equals(other))

	cars.Clear()
	assert.True(t, cars.Empty())

	assert.Equal(t, 3, other.Len())
	assert.True(t, other.Contains("honda"))
	assert.True(t, other.Contains("ford"))
	assert.True(t, other.Contains("jeep"))
}

func TestLogicalSetOperations(t *testing.T) {
	cars := &Set[string]{
		entries: map[string]struct{}{
			"porsche": {},
			"toyota":  {},
			"honda":   {},
			"ford":    {},
			"jeep":    {},
		},
	}

	other := &Set[string]{
		entries: map[string]struct{}{
			"honda": {},
			"ford":  {},
			"jeep":  {},
			"chevy": {},
			"dodge": {},
		},
	}

	expected := &Set[string]{
		entries: map[string]struct{}{
			"honda": {},
			"ford":  {},
			"jeep":  {},
		},
	}
	assert.True(t, cars.Intersection(other).Equals(expected))

	expected = &Set[string]{
		entries: map[string]struct{}{
			// Make sure order does not matter.
			"chevy":   {},
			"dodge":   {},
			"porsche": {},
			"toyota":  {},
		},
	}
	assert.True(t, cars.Disjunction(other).Equals(expected))

	expected = &Set[string]{
		entries: map[string]struct{}{
			"chevy": {},
			"dodge": {},
		},
	}
	assert.True(t, other.Difference(cars).Equals(expected))

	expected = &Set[string]{
		entries: map[string]struct{}{
			"porsche": {},
			"toyota":  {},
		},
	}
	assert.True(t, cars.Difference(other).Equals(expected))

	expected = &Set[string]{
		entries: map[string]struct{}{
			"porsche": {},
			"toyota":  {},
			"honda":   {},
			"ford":    {},
			"jeep":    {},
			"chevy":   {},
			"dodge":   {},
		},
	}
	assert.True(t, cars.Union(other).Equals(expected))

	// Test the sets with imbalanced lengths.
	other = &Set[string]{
		entries: map[string]struct{}{
			"honda": {},
			"dodge": {},
		},
	}

	assert.Equal(t, 1, other.Intersection(cars).Len())
	assert.Equal(t, 6, other.Union(cars).Len())
	assert.False(t, other.Equals(cars))
}

func TestEqualsSameLengthDifferentElements(t *testing.T) {
	assert.False(t, FromSlice([]int{1, 2}).Equals(FromSlice([]int{1, 3})))
}

func TestDisjunction(t *testing.T) {
	cars := FromSlice([]string{"porsche", "toyota", "honda"})
	other := FromSlice([]string{"honda", "ford"})

	expected := FromSlice([]string{"porsche", "toyota", "ford"})
	assert.True(t, cars.Disjunction(other).Equals(expected))
	assert.True(t, other.Disjunction(cars).Equals(expected))

	// The receiver and the argument are not modified.
	assert.Equal(t, 3, cars.Len())
	assert.True(t, cars.Contains("honda"))
	assert.Equal(t, 2, other.Len())

	empty := New[string]()
	assert.True(t, cars.Disjunction(empty).Equals(cars))
}

func TestZeroValueSetOperations(t *testing.T) {
	tests := []struct {
		name           string
		run            func(receiver, argument *Set[int]) *Set[int]
		zeroIsReceiver bool
		expected       []int
	}{
		{"Union, zero receiver", (*Set[int]).Union, true, []int{1, 2}},
		{"Union, zero argument", (*Set[int]).Union, false, []int{1, 2}},
		{"Intersection, zero receiver", (*Set[int]).Intersection, true, []int{}},
		{"Intersection, zero argument", (*Set[int]).Intersection, false, []int{}},
		{"Difference, zero receiver", (*Set[int]).Difference, true, []int{}},
		{"Difference, zero argument", (*Set[int]).Difference, false, []int{1, 2}},
		{"Disjunction, zero receiver", (*Set[int]).Disjunction, true, []int{1, 2}},
		{"Disjunction, zero argument", (*Set[int]).Disjunction, false, []int{1, 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var zero Set[int]
			populated := FromSlice([]int{1, 2})
			receiver, argument := populated, &zero
			if test.zeroIsReceiver {
				receiver, argument = &zero, populated
			}

			result := test.run(receiver, argument)

			elements := result.ToSlice()
			slices.Sort(elements)
			assert.Equal(t, test.expected, elements)

			// The result is a usable, independent Set.
			result.Add(99)
			assert.True(t, result.Contains(99))
			assert.False(t, populated.Contains(99))
			assert.True(t, zero.Empty())
		})
	}
}

func TestZeroValueSetMethods(t *testing.T) {
	var zero Set[int]
	assert.Equal(t, []int{}, zero.ToSlice())

	clone := zero.Clone()
	assert.True(t, clone.Empty())
	clone.Add(99)
	assert.True(t, clone.Contains(99))

	var cleared Set[int]
	cleared.Clear()
	assert.True(t, cleared.Empty())
	cleared.Add(1)
	assert.True(t, cleared.Contains(1))

	var subtracted Set[int]
	subtracted.DifferenceUpdate(FromSlice([]int{1, 2}))
	assert.True(t, subtracted.Empty())
	subtracted.Add(99)
	assert.True(t, subtracted.Contains(99))
}
