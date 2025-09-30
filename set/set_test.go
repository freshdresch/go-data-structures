package set

import (
        "testing"
        "github.com/stretchr/testify/assert"
)

func TestMakingEmptySet(t *testing.T) {
        set := NewSet[string]()
        assert.True(t, set.Empty())
}

func TestMakingSetFromSlice(t *testing.T) {
        inputSlice := []string{
                "one",
                "two",
                "three",
                "three", // the duplicate should not affect the set
        }
        s := NewSetFromSlice[string](inputSlice)
        assert.True(t, s.Exists("one"))
        assert.True(t, s.Exists("two"))
        assert.True(t, s.Exists("three"))
        assert.Equal(t, 3, s.Len())
}

func TestAddAndRemove(t *testing.T) {
        s := NewSet[string]()
        s.Add("porsche")
        s.Add("jaguar")
        s.Add("toyota")

        assert.True(t, s.Exists("porsche"))
        assert.True(t, s.Exists("jaguar"))
        assert.True(t, s.Exists("toyota"))
        assert.False(t, s.Exists("honda"))
        assert.False(t, s.Exists("ford"))
        assert.False(t, s.Exists("jeep"))

        // test the no-op add too
        length := s.Len()
        s.Add("porsche")
        assert.True(t, s.Exists("porsche"))
        assert.Equal(t, length, s.Len())

        other := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 3,
        }

        s.Update(other)

        // test all of the elements, even the ones we didn't expect to change,
        // just to make sure no funny business is happening
        assert.True(t, s.Exists("porsche"))
        assert.True(t, s.Exists("jaguar"))
        assert.True(t, s.Exists("toyota"))
        assert.True(t, s.Exists("honda"))
        assert.True(t, s.Exists("ford"))
        assert.True(t, s.Exists("jeep"))

        s.Remove("porsche")

        assert.False(t, s.Exists("porsche"))
        assert.True(t, s.Exists("jaguar"))
        assert.True(t, s.Exists("toyota"))
        assert.True(t, s.Exists("honda"))
        assert.True(t, s.Exists("ford"))
        assert.True(t, s.Exists("jeep"))

        // test the no-op remove too
        length = s.Len()
        s.Remove("fiat")
        assert.Equal(t, length, s.Len())

        // add one more element to the Set that we are going to bulk subtract
        other.Add("jaguar")
        s.DifferenceUpdate(other)

        assert.False(t, s.Exists("porsche"))
        assert.False(t, s.Exists("jaguar"))
        assert.True(t, s.Exists("toyota"))
        assert.False(t, s.Exists("honda"))
        assert.False(t, s.Exists("ford"))
        assert.False(t, s.Exists("jeep"))

        // test the skipping of values in the bulk update functions
        // now `set` will have `toyota` and `porsche`
        s.Add("porsche")
        assert.Equal(t, 2, s.Len())

        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "porsche": struct{}{},
                },
                length: 2,
        }
        s.Update(other)
        assert.Equal(t, 3, s.Len())

        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "jaguar": struct{}{},
                },
                length: 2,
        }
        s.DifferenceUpdate(other)
        assert.Equal(t, 2, s.Len())
}

func TestCloneSafety(t *testing.T) {
        s := &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "ford": struct{}{},
                        "jeep": struct{}{},
                },
                length: 3,
        }

        other := s.Clone()
        assert.True(t, s.Equals(other))

        s.Clear()
        assert.True(t, s.Empty())

        assert.Equal(t, 3, other.Len())
        assert.True(t, other.Exists("honda"))
        assert.True(t, other.Exists("ford"))
        assert.True(t, other.Exists("jeep"))
}

func TestLogicalSetOperations(t *testing.T) {
        s := &Set[string]{
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
        assert.True(t, s.Intersection(other).Equals(expected))

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
        assert.True(t, s.Disjunction(other).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        "chevy": struct{}{},
                        "dodge": struct{}{},
                },
                length: 2,
        }
        assert.True(t, other.Difference(s).Equals(expected))

        expected = &Set[string]{
                entries: map[string]struct{}{
                        "porsche": struct{}{},
                        "toyota": struct{}{},
                },
                length: 2,
        }
        assert.True(t, s.Difference(other).Equals(expected))

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
        assert.True(t, s.Union(other).Equals(expected))

        // test the sets with imbalanced lengths
        other = &Set[string]{
                entries: map[string]struct{}{
                        "honda": struct{}{},
                        "dodge": struct{}{},
                },
                length: 2,
        }

        assert.Equal(t, 1, other.Intersection(s).Len())
        assert.Equal(t, 6, other.Union(s).Len())
        assert.False(t, other.Equals(s))
}
