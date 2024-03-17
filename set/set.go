package set

// Set is a collection of operations built on top of the golang map, in order to
// provide the primitives one would expect from a Set type.
//
// It maintains its length during every add/remove operation, to avoid the linear
// cost of determining its length with len(entries).
type Set[K comparable] struct {
        entries  map[K]struct{}
        length   uint64
}

// NewSet returns a pointer to a freshly constructed Set, instantiated to its
// zero value.
func NewSet[K comparable]() *Set[K] {
        return &Set[K]{
                entries: make(map[K]struct{}),
                length: 0,
        }
}

// Add adds a single element to the Set. If the element is in the set, the
// operation is a no-op.
func (set *Set[K]) Add(element K) {
        if set.Exists(element) {
                return
        }
        
        set.entries[element] = struct{}{}
        set.length += 1
}

// Remove deletes an element from the Set. If the element is not in the set, the
// operation is a no-op.
func (set *Set[K]) Remove(element K) {
        if !set.Exists(element) {
                return
        }

        delete(set.entries, element)
        set.length -= 1
}

// Difference returns a set containing the set difference between the receiver
// Set and the argument Set (in the direction `receiver - argument`).
//
// Note: This requires a deep-copy and does not modify the receiver. Usually this
// will only be a partial deep-copy, as we avoid copying the elements that would
// be deleted due to the set difference.
func (set *Set[K]) Difference(other *Set[K]) *Set[K] {
        var added uint64

        result := NewSet[K]()
        for element := range set.entries {
                if other.Exists(element) {
                        continue
                }
                result.entries[element] = struct{}{}
                added += 1
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
func (set *Set[K]) Intersection(other *Set[K]) *Set[K] {
        var smallerMap *map[K]struct{}
        var largerMap *map[K]struct{}
        var added uint64

        // iterate through the smaller set
        if set.length < other.length {
                smallerMap = &set.entries
                largerMap = &other.entries
        } else {
                smallerMap = &other.entries
                largerMap = &set.entries
        }

        result := NewSet[K]()
        for element := range *smallerMap {
                if _, ok := (*largerMap)[element]; !ok {
                        continue
                }
                result.entries[element] = struct{}{}
                added += 1
        }

        result.length += added
        return result
}

// Union returns a set containing the result of the union operation between the
// receiver Set and the argument Set.
//
// Note: This requires a deep-copy and does not modify the receiver or argument.
func (set *Set[K]) Union(other *Set[K]) *Set[K] {
        var smallerSet *Set[K]
        var largerSet *Set[K]
        var added uint64

        // we want to deep-copy the larger set to get more work out of the way
        if set.length < other.length {
                smallerSet = set
                largerSet = other
        } else {
                smallerSet = other
                largerSet = set
        }

        result := largerSet.Clone()
        for element := range smallerSet.entries {
                if largerSet.Exists(element) {
                        continue
                }
                result.entries[element] = struct{}{}
                added += 1
        }

        result.length += added
        return result
}

// Disjunction returns a set containing the symmetric difference between the
// receiver Set and the argument Set. This can be calculated as the difference
// between the Union and the Intersection of the two sets.
//
// Note: This requires a deep-copy and does not modify the receiver or argument.
func (set *Set[K]) Disjunction(other *Set[K]) *Set[K] {
        // we will do the deep copy in the Union, so we can modify that copy
        // in-place afterward
        result := set.Union(other)
        intersection := set.Intersection(other)
        for element := range intersection.entries {
                result.Remove(element)
        }
        return result
}

// Exists returns whether or not the element exists in the Set.
func (set *Set[K]) Exists(element K) bool {
        if _, ok := set.entries[element]; !ok {
                return false
        }
        return true
}

// Equals returns whether the two sets have identical contents.
func (set *Set[K]) Equals(other *Set[K]) bool {
        if set.length != other.length {
                return false
        }

        return set.Intersection(other).Len() == set.length
}

// Empty returns whether the Set contains any elements or not.
func (set *Set[K]) Empty() bool {
        return set.length == 0
}

// Len returns the number of elements in the Set.
func (set *Set[K]) Len() uint64 {
        return set.length
}

// Clear empties the map and sets the length to 0.
func (set *Set[K]) Clear() {
        for element := range set.entries {
                delete(set.entries, element)
        }
        set.length = 0
}

// Clone creates a deep copy of the Set.
func (set *Set[K]) Clone() *Set[K] {
        result := &Set[K]{
                entries: make(map[K]struct{}, set.length),
                length: set.length,
        }

        for elem := range set.entries {
                result.entries[elem] = struct{}{}
        }

        return result
}

// Update adds the contents of the argument Set to the receiver Set.
//
// Note: this modifies the receiver Set in-place, but does not modify the
// argument Set.
func (set *Set[K]) Update(other *Set[K]) {
        var added uint64
        for element := range other.entries {
                if set.Exists(element) {
                        continue
                }
                set.entries[element] = struct{}{}
                added += 1
        }
        set.length += added
}

// DifferenceUpdate removes the contents of the argument Set from the receiver
// Set.
//
// Note: this modifies the receiver Set in-place, but does not modify the
// argument Set.
func (set *Set[K]) DifferenceUpdate(other *Set[K]) {
        var removed uint64
        for element := range other.entries {
                if !set.Exists(element) {
                        continue
                }
                delete(set.entries, element)
                removed += 1
        }
        set.length -= removed
}
