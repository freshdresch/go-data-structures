package set

// Set is a collection of operations built on top of the golang map, in order to
// provide the primitives one would expect from a Set type.
//
// It maintains its length during every add/remove operation, to avoid the linear
// cost of determining its length with len(entries).
type Set[K comparable] struct {
        entries  map[K]struct{}
        length   int
}

// NewSet returns a pointer to a freshly constructed Set, instantiated to its
// zero value.
func NewSet[K comparable]() *Set[K] {
        return &Set[K]{
                entries: make(map[K]struct{}),
                length: 0,
        }
}

// NewSetFromSlice returns a pointer to a freshly constructed Set, instantiated
// to the contents of the input slice.
func NewSetFromSlice[K comparable](keys []K) *Set[K] {
        s := Set[K]{
                entries: make(map[K]struct{}),
                length: 0,
        }
        for _, key := range keys {
                s.Add(key)
        }
        return &s
}


// Add adds a single element to the Set. If the element is in the set, the
// operation is a no-op.
func (s *Set[K]) Add(element K) {
        if _, ok := s.entries[element]; ok {
                return
        }
        s.entries[element] = struct{}{}
        s.length++
}

// Remove deletes an element from the Set. If the element is not in the set, the
// operation is a no-op.
func (s *Set[K]) Remove(element K) {
        if _, ok := s.entries[element]; !ok {
                return
        }
        delete(s.entries, element)
        s.length--
}

// Difference returns a set containing the set difference between the receiver
// Set and the argument Set (in the direction `receiver - argument`).
//
// Note: This requires a deep-copy and does not modify the receiver. Usually this
// will only be a partial deep-copy, as we avoid copying the elements that would
// be deleted due to the set difference.
func (s *Set[K]) Difference(other *Set[K]) *Set[K] {
        result := NewSet[K]()
        added := 0
        for element := range s.entries {
                if other.Exists(element) {
                        continue
                }
                result.entries[element] = struct{}{}
                added++
        }

        result.length += added
        return result
}

// Intersection returns a set containing the set intersection between the
// receiver Set and the argument Set.
//
// Note: This requires a deep-copy and does not modify the receiver or argument.
// Usually this will only be a partial deep-copy, as we avoid copying the
// elements that are not resident in both sets.
func (s *Set[K]) Intersection(other *Set[K]) *Set[K] {
        var (
                smallerMap map[K]struct{}
                largerMap  map[K]struct{}
        )

        // iterate through the smaller set
        if s.length < other.length {
                smallerMap = s.entries
                largerMap = other.entries
        } else {
                smallerMap = other.entries
                largerMap = s.entries
        }

        result := NewSet[K]()
        added := 0
        for element := range smallerMap {
                if _, ok := largerMap[element]; !ok {
                        continue
                }
                result.entries[element] = struct{}{}
                added++
        }

        result.length += added
        return result
}

// Union returns a set containing the result of the union operation between the
// receiver Set and the argument Set.
//
// Note: This requires a deep-copy and does not modify the receiver or argument.
func (s *Set[K]) Union(other *Set[K]) *Set[K] {
        var (
                smallerSet *Set[K]
                largerSet  *Set[K]
        )

        // we want to deep-copy the larger set to get more work out of the way
        if s.length < other.length {
                smallerSet = s
                largerSet = other
        } else {
                smallerSet = other
                largerSet = s
        }

        result := largerSet.Clone()
        added := 0
        for element := range smallerSet.entries {
                if _, ok := result.entries[element]; ok {
                        continue
                }
                result.entries[element] = struct{}{}
                added++
        }

        result.length += added
        return result
}

// Disjunction returns a set containing the symmetric difference between the
// receiver Set and the argument Set. This can be calculated as the difference
// between the Union and the Intersection of the two sets.
//
// Note: This requires a deep-copy and does not modify the receiver or argument.
func (s *Set[K]) Disjunction(other *Set[K]) *Set[K] {
        // we will do the deep copy in the Union, so we can modify that copy
        // in-place afterward
        result := s.Union(other)
        intersection := s.Intersection(other)
        for element := range intersection.entries {
                result.Remove(element)
        }
        return result
}

// Exists returns whether or not the element exists in the Set.
func (s *Set[K]) Exists(element K) bool {
        _, ok := s.entries[element]
        return ok
}

// Equals returns whether the two sets have identical contents.
func (s *Set[K]) Equals(other *Set[K]) bool {
        if s.length != other.length {
                return false
        }

        return s.Intersection(other).Len() == s.length
}

// Empty returns whether the Set contains any elements or not.
func (s *Set[K]) Empty() bool {
        return s.length == 0
}

// Len returns the number of elements in the Set.
func (s *Set[K]) Len() int {
        return s.length
}

// Clear empties the map and sets the length to 0.
func (s *Set[K]) Clear() {
        s.entries = make(map[K]struct{})
        s.length = 0
}

// Clone creates a deep copy of the Set.
func (s *Set[K]) Clone() *Set[K] {
        result := &Set[K]{
                entries: make(map[K]struct{}, s.length),
                length:  s.length,
        }

        for elem := range s.entries {
                result.entries[elem] = struct{}{}
        }

        return result
}

// Update adds the contents of the argument Set to the receiver Set.
//
// Note: this modifies the receiver Set in-place, but does not modify the
// argument Set.
func (s *Set[K]) Update(other *Set[K]) {
        added := 0
        for element := range other.entries {
                if _, ok := s.entries[element]; ok {
                        continue
                }
                s.entries[element] = struct{}{}
                added++
        }
        s.length += added
}

// DifferenceUpdate removes the contents of the argument Set from the receiver
// Set.
//
// Note: this modifies the receiver Set in-place, but does not modify the
// argument Set.
func (s *Set[K]) DifferenceUpdate(other *Set[K]) {
        removed := 0
        for element := range other.entries {
                if _, ok := s.entries[element]; !ok {
                        continue
                }
                delete(s.entries, element)
                removed++
        }
        s.length -= removed
}
