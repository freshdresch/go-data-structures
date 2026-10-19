package set

import (
	"cmp"
	"iter"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakingEmptySet(t *testing.T) {
	empty := New[string]()
	assert.True(t, empty.IsEmpty())
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
	assert.True(t, zero.IsEmpty())

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
	assert.True(t, cars.IsEmpty())

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
			assert.True(t, zero.IsEmpty())
		})
	}
}

func TestZeroValueSetMethods(t *testing.T) {
	var zero Set[int]
	assert.Equal(t, []int{}, zero.ToSlice())

	clone := zero.Clone()
	assert.True(t, clone.IsEmpty())
	clone.Add(99)
	assert.True(t, clone.Contains(99))

	var cleared Set[int]
	cleared.Clear()
	assert.True(t, cleared.IsEmpty())
	cleared.Add(1)
	assert.True(t, cleared.Contains(1))

	var subtracted Set[int]
	subtracted.DifferenceUpdate(FromSlice([]int{1, 2}))
	assert.True(t, subtracted.IsEmpty())
	subtracted.Add(99)
	assert.True(t, subtracted.Contains(99))
}

// isEven is a named function type, to check that Predicate accepts it.
type isEven func(int) bool

// sortedElements returns the elements of set in ascending order.
func sortedElements[K cmp.Ordered](set *Set[K]) []K {
	elements := set.ToSlice()
	slices.Sort(elements)
	return elements
}

// yieldsTwice yields 7 twice, to check that duplicates are stored once.
var yieldsTwice iter.Seq[int] = func(yield func(int) bool) {
	if !yield(7) {
		return
	}
	yield(7)
}

func TestContainsFunc(t *testing.T) {
	even := func(element int) bool { return element%2 == 0 }

	var zero Set[int]
	assert.False(t, zero.ContainsFunc(even))
	assert.False(t, New[int]().ContainsFunc(even))

	odds := FromSlice([]int{1, 3, 5})
	assert.False(t, odds.ContainsFunc(even))
	assert.True(t, odds.ContainsFunc(func(int) bool { return true }))

	mixed := FromSlice([]int{1, 2, 3})
	assert.True(t, mixed.ContainsFunc(even))

	calls := 0
	countCalls := func(int) bool {
		calls++
		return true
	}
	assert.True(t, FromSlice([]int{1, 2, 3}).ContainsFunc(countCalls))
	assert.Equal(t, 1, calls)
}

func TestRemoveFunc(t *testing.T) {
	even := func(element int) bool { return element%2 == 0 }

	mixed := FromSlice([]int{1, 2, 3, 4})
	mixed.RemoveFunc(even)
	assert.Equal(t, []int{1, 3}, sortedElements(mixed))

	all := FromSlice([]int{2, 4})
	all.RemoveFunc(even)
	assert.True(t, all.IsEmpty())

	none := FromSlice([]int{1, 3})
	none.RemoveFunc(even)
	assert.Equal(t, []int{1, 3}, sortedElements(none))

	var zero Set[int]
	zero.RemoveFunc(even)
	assert.True(t, zero.IsEmpty())

	empty := New[int]()
	empty.RemoveFunc(even)
	assert.True(t, empty.IsEmpty())
}

func TestFilter(t *testing.T) {
	even := func(element int) bool { return element%2 == 0 }
	numbers := FromSlice([]int{1, 2, 3, 4})

	evens := numbers.Filter(even)
	assert.Equal(t, []int{2, 4}, sortedElements(evens))
	assert.Equal(t, []int{1, 2, 3, 4}, sortedElements(numbers))

	none := numbers.Filter(func(int) bool { return false })
	assert.True(t, none.IsEmpty())
	none.Add(99)
	assert.True(t, none.Contains(99))

	all := numbers.Filter(func(int) bool { return true })
	assert.Equal(t, []int{1, 2, 3, 4}, sortedElements(all))
	all.Add(100)
	assert.False(t, numbers.Contains(100))

	var zero Set[int]
	zeroResult := zero.Filter(even)
	assert.True(t, zeroResult.IsEmpty())
	zeroResult.Add(1)
	assert.True(t, zero.IsEmpty())
	assert.True(t, New[int]().Filter(even).IsEmpty())

	// Adding to the result does not change the receiver.
	evens.Add(100)
	assert.False(t, numbers.Contains(100))
}

func TestPredicateKinds(t *testing.T) {
	plain := func(element int) bool { return element%2 == 0 }
	tests := []struct {
		name      string
		predicate Predicate[int]
	}{
		{"function literal", func(element int) bool { return element%2 == 0 }},
		{"plain function variable", plain},
		{"named function type", isEven(plain)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			numbers := FromSlice([]int{1, 2, 3, 4})
			assert.True(t, numbers.ContainsFunc(test.predicate))
			assert.Equal(t, []int{2, 4}, sortedElements(numbers.Filter(test.predicate)))

			numbers.RemoveFunc(test.predicate)
			assert.Equal(t, []int{1, 3}, sortedElements(numbers))
		})
	}
}

func TestOf(t *testing.T) {
	empty := Of[int]()
	assert.True(t, empty.IsEmpty())
	empty.Add(1)
	assert.True(t, empty.Contains(1))

	numbers := Of(1, 2, 2, 3)
	assert.Equal(t, []int{1, 2, 3}, sortedElements(numbers))

	input := []int{1, 2}
	spread := Of(input...)
	input[0] = 99
	assert.Equal(t, []int{1, 2}, sortedElements(spread))
	assert.False(t, spread.Contains(99))
}

func TestCollect(t *testing.T) {
	cars := FromSlice([]string{"porsche", "toyota"})
	copied := Collect(cars.All())
	assert.Equal(t, []string{"porsche", "toyota"}, sortedElements(copied))

	counts := map[string]int{"honda": 1, "ford": 2}
	keys := Collect(maps.Keys(counts))
	assert.Equal(t, []string{"ford", "honda"}, sortedElements(keys))

	empty := Collect(slices.Values([]int{}))
	assert.True(t, empty.IsEmpty())
	empty.Add(1)
	assert.True(t, empty.Contains(1))

	duplicates := Collect(yieldsTwice)
	assert.Equal(t, []int{7}, sortedElements(duplicates))
}

func TestAddAll(t *testing.T) {
	target := FromSlice([]int{3})

	target.AddAll(FromSlice([]int{1, 2}).All())
	assert.Equal(t, []int{1, 2, 3}, sortedElements(target))

	target.AddAll(slices.Values([]int{}))
	assert.Equal(t, []int{1, 2, 3}, sortedElements(target))

	target.AddAll(yieldsTwice)
	assert.Equal(t, []int{1, 2, 3, 7}, sortedElements(target))

	var zero Set[int]
	zero.AddAll(slices.Values([]int{5}))
	assert.Equal(t, []int{5}, sortedElements(&zero))

	var emptyTarget Set[int]
	emptyTarget.AddAll(slices.Values([]int{}))
	assert.True(t, emptyTarget.IsEmpty())
	emptyTarget.Add(1)
	assert.True(t, emptyTarget.Contains(1))

	// Adding a set's own elements leaves it unchanged.
	self := FromSlice([]int{1, 2})
	self.AddAll(self.All())
	assert.Equal(t, []int{1, 2}, sortedElements(self))
}

func TestRemoveAll(t *testing.T) {
	target := FromSlice([]int{1, 2, 3, 4})

	target.RemoveAll(FromSlice([]int{2, 4}).All())
	assert.Equal(t, []int{1, 3}, sortedElements(target))

	target.RemoveAll(slices.Values([]int{}))
	assert.Equal(t, []int{1, 3}, sortedElements(target))

	target.RemoveAll(slices.Values([]int{99}))
	assert.Equal(t, []int{1, 3}, sortedElements(target))

	target.RemoveAll(slices.Values([]int{1, 1}))
	assert.Equal(t, []int{3}, sortedElements(target))

	var zero Set[int]
	zero.RemoveAll(slices.Values([]int{1}))
	assert.True(t, zero.IsEmpty())
	zero.Add(1)
	assert.True(t, zero.Contains(1))

	// Removing a set's own elements empties it.
	self := FromSlice([]int{1, 2})
	self.RemoveAll(self.All())
	assert.True(t, self.IsEmpty())
}

// relationCase is a receiver and argument pair with the expected result of each relation.
// The sets are built by newPair, so each test gets its own copies to modify.
type relationCase struct {
	name       string
	newPair    func() (receiver, argument *Set[int])
	isSubset   bool
	isSuperset bool
	isDisjoint bool
	equals     bool
}

// pairOf returns a newPair function for two distinct sets holding the given elements.
func pairOf(receiver, argument []int) func() (*Set[int], *Set[int]) {
	return func() (*Set[int], *Set[int]) {
		return FromSlice(receiver), FromSlice(argument)
	}
}

func relationCases() []relationCase {
	return []relationCase{
		{
			name:       "equal sets",
			newPair:    pairOf([]int{1, 2}, []int{1, 2}),
			isSubset:   true,
			isSuperset: true,
			isDisjoint: false,
			equals:     true,
		},
		{
			name:       "proper subset",
			newPair:    pairOf([]int{1}, []int{1, 2}),
			isSubset:   true,
			isSuperset: false,
			isDisjoint: false,
			equals:     false,
		},
		{
			name:       "proper superset",
			newPair:    pairOf([]int{1, 2}, []int{1}),
			isSubset:   false,
			isSuperset: true,
			isDisjoint: false,
			equals:     false,
		},
		{
			name:       "overlapping",
			newPair:    pairOf([]int{1, 2}, []int{2, 3}),
			isSubset:   false,
			isSuperset: false,
			isDisjoint: false,
			equals:     false,
		},
		{
			name:       "disjoint",
			newPair:    pairOf([]int{1, 2}, []int{3, 4}),
			isSubset:   false,
			isSuperset: false,
			isDisjoint: true,
			equals:     false,
		},
		{
			name:       "empty receiver",
			newPair:    pairOf([]int{}, []int{1, 2}),
			isSubset:   true,
			isSuperset: false,
			isDisjoint: true,
			equals:     false,
		},
		{
			name:       "empty argument",
			newPair:    pairOf([]int{1, 2}, []int{}),
			isSubset:   false,
			isSuperset: true,
			isDisjoint: true,
			equals:     false,
		},
		{
			name: "two zero values",
			newPair: func() (*Set[int], *Set[int]) {
				return &Set[int]{}, &Set[int]{}
			},
			isSubset:   true,
			isSuperset: true,
			isDisjoint: true,
			equals:     true,
		},
		{
			name: "zero receiver",
			newPair: func() (*Set[int], *Set[int]) {
				return &Set[int]{}, FromSlice([]int{1, 2})
			},
			isSubset:   true,
			isSuperset: false,
			isDisjoint: true,
			equals:     false,
		},
		{
			name: "zero argument",
			newPair: func() (*Set[int], *Set[int]) {
				return FromSlice([]int{1, 2}), &Set[int]{}
			},
			isSubset:   false,
			isSuperset: true,
			isDisjoint: true,
			equals:     false,
		},
		{
			name: "same non-empty set",
			newPair: func() (*Set[int], *Set[int]) {
				same := FromSlice([]int{1, 2})
				return same, same
			},
			isSubset:   true,
			isSuperset: true,
			isDisjoint: false,
			equals:     true,
		},
		{
			name: "same zero-value set",
			newPair: func() (*Set[int], *Set[int]) {
				same := &Set[int]{}
				return same, same
			},
			isSubset:   true,
			isSuperset: true,
			isDisjoint: true,
			equals:     true,
		},
	}
}

func TestRelations(t *testing.T) {
	for _, test := range relationCases() {
		t.Run(test.name, func(t *testing.T) {
			receiver, argument := test.newPair()
			assert.Equal(t, test.isSubset, receiver.IsSubset(argument))
			assert.Equal(t, test.isSuperset, receiver.IsSuperset(argument))
			assert.Equal(t, test.isDisjoint, receiver.IsDisjoint(argument))
			assert.Equal(t, test.equals, receiver.Equals(argument))
		})
	}
}

func TestContainsAll(t *testing.T) {
	numbers := FromSlice([]int{1, 2, 3})

	assert.True(t, numbers.ContainsAll(FromSlice([]int{1, 3}).All()))
	assert.False(t, numbers.ContainsAll(slices.Values([]int{1, 4})))
	assert.False(t, numbers.ContainsAll(slices.Values([]int{4, 5})))
	assert.True(t, numbers.ContainsAll(slices.Values([]int{})))
	assert.True(t, numbers.ContainsAll(slices.Values([]int{2, 2})))
	assert.True(t, FromSlice([]int{7}).ContainsAll(yieldsTwice))

	var zero Set[int]
	assert.True(t, zero.ContainsAll(slices.Values([]int{})))
	assert.False(t, zero.ContainsAll(slices.Values([]int{1})))

	reads := 0
	missingFirst := func(yield func(int) bool) {
		for _, element := range []int{99, 1, 2} {
			reads++
			if !yield(element) {
				return
			}
		}
	}
	assert.False(t, numbers.ContainsAll(missingFirst))
	assert.Equal(t, 1, reads)
}

func TestInPlaceOperationsMatchCopyingForms(t *testing.T) {
	for _, test := range relationCases() {
		t.Run(test.name, func(t *testing.T) {
			receiver, argument := test.newPair()
			if receiver == argument {
				original := sortedElements(receiver)
				receiver.IntersectionUpdate(receiver)
				assert.Equal(t, original, sortedElements(receiver))
				receiver.DisjunctionUpdate(receiver)
				assert.True(t, receiver.IsEmpty())
				return
			}

			originalArgument := sortedElements(argument)

			intersected := receiver.Clone()
			intersected.IntersectionUpdate(argument)
			expected := sortedElements(receiver.Intersection(argument))
			assert.Equal(t, expected, sortedElements(intersected))
			assert.Equal(t, originalArgument, sortedElements(argument))

			disjoined := receiver.Clone()
			disjoined.DisjunctionUpdate(argument)
			expected = sortedElements(receiver.Disjunction(argument))
			assert.Equal(t, expected, sortedElements(disjoined))
			assert.Equal(t, originalArgument, sortedElements(argument))
		})
	}
}

func TestDisjunctionUpdateZeroValueReceiver(t *testing.T) {
	var zero Set[int]
	zero.DisjunctionUpdate(FromSlice([]int{1, 2}))
	assert.Equal(t, []int{1, 2}, sortedElements(&zero))
}

func TestIntersectionUpdateZeroValueReceiver(t *testing.T) {
	var zero Set[int]
	zero.IntersectionUpdate(FromSlice([]int{1, 2}))
	assert.True(t, zero.IsEmpty())
	zero.Add(3)
	assert.True(t, zero.Contains(3))
}
