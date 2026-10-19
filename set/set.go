// Package set provides a generic Set type built on a Go map.
package set

import "iter"

// Set is a collection of unique elements built on a Go map. The zero value is an
// empty Set that is ready to use. A Set must not be copied after first use. Pass a
// *Set instead.
type Set[K comparable] struct {
	entries map[K]struct{}
}

// New returns a pointer to a new, empty Set. The zero value of Set is also an empty,
// ready-to-use Set.
func New[K comparable]() *Set[K] {
	return &Set[K]{
		entries: make(map[K]struct{}),
	}
}

// FromSlice returns a pointer to a new Set holding the elements of keys. Duplicate
// elements are stored once. FromSlice does not modify keys.
func FromSlice[K comparable](keys []K) *Set[K] {
	result := New[K]()
	for _, key := range keys {
		result.Add(key)
	}
	return result
}

// Add adds a single element to the Set. If the element is already in the Set, Add
// does nothing. Add modifies the receiver.
func (s *Set[K]) Add(element K) {
	if s.entries == nil {
		s.entries = make(map[K]struct{})
	}
	s.entries[element] = struct{}{}
}

// Remove deletes an element from the Set. If the element is not in the Set, Remove
// does nothing. Remove modifies the receiver.
func (s *Set[K]) Remove(element K) {
	delete(s.entries, element)
}

// Difference returns a new Set with the elements of the receiver that are not in
// other. The result is in the direction `receiver - other`.
//
// Difference copies the elements into a new Set, so the result does not share storage
// with either Set. Elements are copied by value, so pointer elements still point to
// the same values. Difference does not modify the receiver or other.
func (s *Set[K]) Difference(other *Set[K]) *Set[K] {
	result := New[K]()
	for element := range s.entries {
		if other.Contains(element) {
			continue
		}
		result.entries[element] = struct{}{}
	}
	return result
}

// Intersection returns a new Set with the elements that are in both the receiver and
// other.
//
// Intersection copies the elements into a new Set, so the result does not share
// storage with either Set. Elements are copied by value, so pointer elements still
// point to the same values. Intersection does not modify the receiver or other.
func (s *Set[K]) Intersection(other *Set[K]) *Set[K] {
	var (
		smallerMap map[K]struct{}
		largerMap  map[K]struct{}
	)

	if len(s.entries) < len(other.entries) {
		smallerMap = s.entries
		largerMap = other.entries
	} else {
		smallerMap = other.entries
		largerMap = s.entries
	}

	result := New[K]()
	for element := range smallerMap {
		if _, ok := largerMap[element]; !ok {
			continue
		}
		result.entries[element] = struct{}{}
	}
	return result
}

// Union returns a new Set with the elements that are in the receiver, other, or both.
//
// Union copies the elements into a new Set, so the result does not share storage
// with either Set. Elements are copied by value, so pointer elements still point to
// the same values. Union does not modify the receiver or other.
func (s *Set[K]) Union(other *Set[K]) *Set[K] {
	var (
		smallerSet *Set[K]
		largerSet  *Set[K]
	)

	// Cloning the larger set leaves fewer elements to add one at a time.
	if len(s.entries) < len(other.entries) {
		smallerSet = s
		largerSet = other
	} else {
		smallerSet = other
		largerSet = s
	}

	result := largerSet.Clone()
	for element := range smallerSet.entries {
		result.entries[element] = struct{}{}
	}
	return result
}

// SymmetricDifference returns a new Set with the elements that are in exactly one of
// the receiver and other. It is calculated as the Union minus the Intersection.
//
// SymmetricDifference copies the elements into a new Set, so the result does not share
// storage with either Set. Elements are copied by value, so pointer elements still
// point to the same values. SymmetricDifference does not modify the receiver or other.
func (s *Set[K]) SymmetricDifference(other *Set[K]) *Set[K] {
	// Union returns a new Set, so the result can be modified in place afterward.
	result := s.Union(other)
	intersection := s.Intersection(other)
	for element := range intersection.entries {
		result.Remove(element)
	}
	return result
}

// Contains reports whether the element is in the Set. Contains does not modify the
// receiver.
func (s *Set[K]) Contains(element K) bool {
	_, ok := s.entries[element]
	return ok
}

// Equals reports whether the two Sets have identical contents. Equals does not modify
// the receiver or other.
func (s *Set[K]) Equals(other *Set[K]) bool {
	if len(s.entries) != len(other.entries) {
		return false
	}
	for element := range s.entries {
		if !other.Contains(element) {
			return false
		}
	}
	return true
}

// Empty reports whether the Set has no elements. Empty does not modify the receiver.
func (s *Set[K]) Empty() bool {
	return len(s.entries) == 0
}

// Len returns the number of elements in the Set. Len does not modify the receiver.
func (s *Set[K]) Len() int {
	return len(s.entries)
}

// All returns an iterator over the elements of the Set, in unspecified order. Each
// element is yielded once, and iteration stops early when yield returns false. The
// Set must not be modified while the iterator is in use. All does not modify the
// receiver.
func (s *Set[K]) All() iter.Seq[K] {
	return func(yield func(K) bool) {
		for element := range s.entries {
			if !yield(element) {
				return
			}
		}
	}
}

// ToSlice returns the elements of the Set in unspecified order, in a newly allocated
// slice with capacity for every element. ToSlice does not modify the receiver.
func (s *Set[K]) ToSlice() []K {
	elements := make([]K, 0, len(s.entries))
	for element := range s.entries {
		elements = append(elements, element)
	}
	return elements
}

// Clear removes every element from the Set. It replaces the underlying map, which
// releases the old storage to the garbage collector. Clear modifies the receiver.
func (s *Set[K]) Clear() {
	s.entries = make(map[K]struct{})
}

// Clone returns a new Set with the same elements as the receiver. The elements are
// copied by value, so pointer elements still point to the same values. Clone does not
// modify the receiver.
func (s *Set[K]) Clone() *Set[K] {
	result := &Set[K]{
		entries: make(map[K]struct{}, len(s.entries)),
	}

	for element := range s.entries {
		result.entries[element] = struct{}{}
	}

	return result
}

// Update adds the elements of other to the receiver. Update modifies the receiver in
// place and does not modify other.
func (s *Set[K]) Update(other *Set[K]) {
	if s.entries == nil {
		s.entries = make(map[K]struct{})
	}
	for element := range other.entries {
		s.entries[element] = struct{}{}
	}
}

// DifferenceUpdate removes the elements of other from the receiver. DifferenceUpdate
// modifies the receiver in place and does not modify other.
func (s *Set[K]) DifferenceUpdate(other *Set[K]) {
	for element := range other.entries {
		delete(s.entries, element)
	}
}
