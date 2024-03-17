package set

import (
        "testing"
        "github.com/stretchr/testify/assert"
)

func TestMakingEmptySet(t *testing.T) {
        set := NewSet[string]()
        assert.True(t, set.Empty())
}

func TestAddAndRemove(t *testing.T) {
        set := NewSet[string]()
        set.Add("porsche")
        set.Add("jaguar")
        set.Add("toyota")

        assert.True(t, set.Exists("porsche"))
        assert.True(t, set.Exists("jaguar"))
        assert.True(t, set.Exists("toyota"))
        assert.False(t, set.Exists("honda"))
        assert.False(t, set.Exists("ford"))
        assert.False(t, set.Exists("jeep"))

        // test the no-op add too
        length := set.Len()
        set.Add("porsche")
        assert.True(t, set.Exists("porsche"))
        assert.Equal(t, length, set.Len())

        other := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 3,
        }
        
        set.Update(other)

        // test all of the elements, even the ones we didn't expect to change,
        // just to make sure no funny business is happening
        assert.True(t, set.Exists("porsche"))
        assert.True(t, set.Exists("jaguar"))
        assert.True(t, set.Exists("toyota"))
        assert.True(t, set.Exists("honda"))
        assert.True(t, set.Exists("ford"))
        assert.True(t, set.Exists("jeep"))

        set.Remove("porsche")

        assert.False(t, set.Exists("porsche"))
        assert.True(t, set.Exists("jaguar"))
        assert.True(t, set.Exists("toyota"))
        assert.True(t, set.Exists("honda"))
        assert.True(t, set.Exists("ford"))
        assert.True(t, set.Exists("jeep"))

        // test the no-op remove too
        length = set.Len()
        set.Remove("fiat")
        assert.Equal(t, length, set.Len())

        // add one more element to the Set that we are going to bulk subtract
        other.Add("jaguar")
        set.DifferenceUpdate(other)

        assert.False(t, set.Exists("porsche"))
        assert.False(t, set.Exists("jaguar"))
        assert.True(t, set.Exists("toyota"))
        assert.False(t, set.Exists("honda"))
        assert.False(t, set.Exists("ford"))
        assert.False(t, set.Exists("jeep"))

        // test the skipping of values in the bulk update functions
        // now `set` will have `toyota` and `porsche`
        set.Add("porsche")
        assert.Equal(t, set.Len(), uint64(2))

        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "porsche": struct{}{},
                },
                length: 2,
        }
        set.Update(other)
        assert.Equal(t, set.Len(), uint64(3))

        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "jaguar": struct{}{},
                },
                length: 2,
        }
        set.DifferenceUpdate(other)
        assert.Equal(t, set.Len(), uint64(2))
}

func TestCloneSafety(t *testing.T) {
        set := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 3,
        }

        other := set.Clone()
        assert.True(t, set.Equals(other))

        set.Clear()
        assert.True(t, set.Empty())

        assert.Equal(t, other.Len(), uint64(3))
        assert.True(t, other.Exists("honda"))
        assert.True(t, other.Exists("ford"))
        assert.True(t, other.Exists("jeep"))
}

func TestLogicalSetOperations(t *testing.T) {
        set := &Set[string]{
                entries: map[string]struct{}{
                        "porsche": struct{}{},
                        "toyota": struct{}{},
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 5,
        }

        other := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                        "chevy": struct{}{},
                        "dodge": struct{}{},
                },
                length: 5,
        }

        expected := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 3,
        }
        assert.True(t, set.Intersection(other).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        // make sure order doesn't matter
                        "chevy": struct{}{},
                        "dodge": struct{}{},
                        "porsche": struct{}{},
                        "toyota": struct{}{},
                },
                length: 4,
        }
        assert.True(t, set.Disjunction(other).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        "chevy": struct{}{},
                        "dodge": struct{}{},
                },
                length: 2,
        }
        assert.True(t, other.Difference(set).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        "porsche": struct{}{},
                        "toyota": struct{}{},
                },
                length: 2,
        }
        assert.True(t, set.Difference(other).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        "porsche": struct{}{},
                        "toyota": struct{}{},
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                        "chevy": struct{}{},
                        "dodge": struct{}{},
                },
                length: 7,
        }
        assert.True(t, set.Union(other).Equals(expected))

        // test the sets with imbalanced lengths
        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "dodge": struct{}{},
                },
                length: 2,
        }

        assert.Equal(t, other.Intersection(set).Len(), uint64(1))
        assert.Equal(t, other.Union(set).Len(), uint64(6))
        assert.False(t, other.Equals(set))
}
