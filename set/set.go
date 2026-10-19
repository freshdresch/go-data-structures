// Package set provides a generic Set type built on a Go map.
//
// This package does not have any special handling for non-reflexive elements (elements e
// where e != e), such as floating-point NaNs.
package set

import (
	"iter"
	"maps"
)

// Predicate reports whether element satisfies a condition. It is an alias, so any function
// of this shape can be passed, including a value of the caller's own named function type.
type Predicate[K any] = func(element K) bool

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

// Of returns a pointer to a new Set holding the elements. Duplicate elements are stored
// once. Of does not keep a reference to elements, so changing the slice afterward does
// not change the Set.
func Of[K comparable](elements ...K) *Set[K] {
	result := &Set[K]{
		entries: make(map[K]struct{}, len(elements)),
	}
	for _, element := range elements {
		result.Add(element)
	}
	return result
}

// FromSlice returns a pointer to a new Set holding the elements of keys. Duplicate
// elements are stored once. FromSlice does not modify keys.
func FromSlice[K comparable](keys []K) *Set[K] {
	return Of(keys...)
}

// Collect returns a pointer to a new Set holding the elements yielded by seq. Duplicate
// elements are stored once, as with maps.Collect. For an empty sequence, Collect returns
// a usable empty Set.
func Collect[K comparable](seq iter.Seq[K]) *Set[K] {
	result := New[K]()
	result.AddAll(seq)
	return result
}

// ensureEntries creates the underlying map if the Set is a zero value. Every method that
// adds to the receiver calls it, so the zero value stays usable.
func (s *Set[K]) ensureEntries() {
	if s.entries == nil {
		s.entries = make(map[K]struct{})
	}
}

// Add adds a single element to the Set. If the element is already in the Set, Add
// does nothing. Add modifies the receiver.
func (s *Set[K]) Add(element K) {
	s.ensureEntries()
	s.entries[element] = struct{}{}
}

// Remove deletes an element from the Set. If the element is not in the Set, Remove
// does nothing. Remove modifies the receiver.
func (s *Set[K]) Remove(element K) {
	delete(s.entries, element)
}

// RemoveFunc deletes every element of the Set for which predicate returns true. It is
// the Set form of Java's removeIf. RemoveFunc modifies the receiver, and predicate must
// not modify the Set.
func (s *Set[K]) RemoveFunc(predicate Predicate[K]) {
	maps.DeleteFunc(s.entries, func(element K, _ struct{}) bool {
		return predicate(element)
	})
}

// smallerFirst returns the Set with fewer elements first. When both have the same number
// of elements, it returns second first.
func smallerFirst[K comparable](first, second *Set[K]) (smaller, larger *Set[K]) {
	if len(first.entries) < len(second.entries) {
		return first, second
	}
	return second, first
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
	smaller, larger := smallerFirst(s, other)
	result := New[K]()
	for element := range smaller.entries {
		if !larger.Contains(element) {
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
	// Cloning the larger set leaves fewer elements to add one at a time.
	smaller, larger := smallerFirst(s, other)
	result := larger.Clone()
	for element := range smaller.entries {
		result.entries[element] = struct{}{}
	}
	return result
}

// Disjunction returns a new Set with the elements that are in exactly one of the
// receiver and other. This is the symmetric difference, also called the disjunctive
// union. It is calculated as the Union minus the Intersection.
//
// Disjunction copies the elements into a new Set, so the result does not share storage
// with either Set. Elements are copied by value, so pointer elements still point to the
// same values. Disjunction does not modify the receiver or other.
func (s *Set[K]) Disjunction(other *Set[K]) *Set[K] {
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

// ContainsFunc reports whether any element of the Set satisfies predicate. It stops at
// the first element for which predicate returns true, so predicate might not be called for
// every element. ContainsFunc does not modify the receiver, and predicate must not modify
// the Set.
func (s *Set[K]) ContainsFunc(predicate Predicate[K]) bool {
	for element := range s.entries {
		if predicate(element) {
			return true
		}
	}
	return false
}

// Filter returns a new Set holding the elements for which predicate returns true. These
// are the elements that RemoveFunc would delete.
//
// Filter copies the elements into a new Set, so the result does not share storage with
// the receiver. Elements are copied by value, so pointer elements still point to the same
// values. Filter does not modify the receiver, and predicate must not modify the Set.
func (s *Set[K]) Filter(predicate Predicate[K]) *Set[K] {
	result := New[K]()
	for element := range s.entries {
		if predicate(element) {
			result.entries[element] = struct{}{}
		}
	}
	return result
}

// Equals reports whether the two Sets have identical contents. Equals does not modify
// the receiver or other.
func (s *Set[K]) Equals(other *Set[K]) bool {
	return len(s.entries) == len(other.entries) && s.IsSubset(other)
}

// IsSubset reports whether every element of the receiver is in other. An empty Set is a
// subset of every Set. IsSubset does not modify the receiver or other.
func (s *Set[K]) IsSubset(other *Set[K]) bool {
	if len(s.entries) > len(other.entries) {
		return false
	}
	return other.ContainsAll(s.All())
}

// IsSuperset reports whether every element of other is in the receiver. Every Set is a
// superset of an empty Set. IsSuperset does not modify the receiver or other.
func (s *Set[K]) IsSuperset(other *Set[K]) bool {
	return other.IsSubset(s)
}

// IsDisjoint reports whether the receiver and other have no element in common. An empty
// Set is disjoint from every Set, including itself. IsDisjoint does not modify the
// receiver or other.
func (s *Set[K]) IsDisjoint(other *Set[K]) bool {
	smaller, larger := smallerFirst(s, other)
	for element := range smaller.entries {
		if larger.Contains(element) {
			return false
		}
	}
	return true
}

// IsEmpty reports whether the Set has no elements. IsEmpty does not modify the receiver.
func (s *Set[K]) IsEmpty() bool {
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
	s.ensureEntries()
	for element := range other.entries {
		s.entries[element] = struct{}{}
	}
}

// AddAll adds every element yielded by seq to the receiver. It is the sequence form of
// Update. s.AddAll(s.All()) is allowed, because it only rewrites existing elements.
// Otherwise seq must not modify or iterate the receiver while AddAll runs. AddAll modifies
// the receiver in place.
func (s *Set[K]) AddAll(seq iter.Seq[K]) {
	s.ensureEntries()
	for element := range seq {
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

// RemoveAll deletes every element yielded by seq from the receiver. It is the sequence
// form of DifferenceUpdate. It removes only the elements seq yields. Use Clear to empty the
// Set. A sequence derived from the receiver is safe, so s.RemoveAll(s.All()) empties s.
// RemoveAll modifies the receiver in place.
func (s *Set[K]) RemoveAll(seq iter.Seq[K]) {
	for element := range seq {
		delete(s.entries, element)
	}
}

// IntersectionUpdate removes from the receiver every element that is not in other. It is
// the in-place form of Intersection and the Set form of Java's retainAll. It costs time in
// proportion to the smaller of the two Sets. It replaces the underlying map, which releases
// the old storage to the garbage collector. IntersectionUpdate modifies the receiver in
// place and does not modify other.
func (s *Set[K]) IntersectionUpdate(other *Set[K]) {
	smaller, larger := smallerFirst(s, other)
	kept := make(map[K]struct{}, len(smaller.entries))
	for element := range smaller.entries {
		if larger.Contains(element) {
			kept[element] = struct{}{}
		}
	}
	s.entries = kept
}

// DisjunctionUpdate sets the receiver to the elements that are in exactly one of the
// receiver and other. It is the in-place form of Disjunction. Passing the receiver itself
// as other empties the receiver. DisjunctionUpdate modifies the receiver in place and does
// not modify other.
func (s *Set[K]) DisjunctionUpdate(other *Set[K]) {
	// Clearing gives the same result as deleting every element, and releases the old storage.
	if other == s {
		s.Clear()
		return
	}

	s.ensureEntries()
	for element := range other.entries {
		if s.Contains(element) {
			delete(s.entries, element)
			continue
		}
		s.entries[element] = struct{}{}
	}
}

// ContainsAll reports whether every element yielded by seq is in the receiver. It is the
// sequence form of IsSuperset. It stops reading seq at the first element the receiver does
// not contain, and an empty sequence gives true. ContainsAll does not modify the receiver.
func (s *Set[K]) ContainsAll(seq iter.Seq[K]) bool {
	for element := range seq {
		if !s.Contains(element) {
			return false
		}
	}
	return true
}
