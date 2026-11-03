# set

`set` provides `Set[K comparable]`, a generic set of unique elements stored in a Go map. It covers
construction from values, slices, and iterators, the standard set algebra in both copying and
in-place forms, and membership and containment tests.

```sh
go get github.com/freshdresch/go-data-structures/set
```

## Example

```go
package main

import (
	"fmt"
	"slices"

	"github.com/freshdresch/go-data-structures/set"
)

func main() {
	wanted := set.Of("read", "write", "admin")
	granted := set.Of("read")

	missing := wanted.Difference(granted)
	fmt.Println(slices.Sorted(missing.All()))

	granted.Update(missing)
	fmt.Println(granted.Equals(wanted))
}
```

Output:

```text
[admin write]
true
```

## Usage rules

- The zero value of `Set` is an empty set that is ready to use. `New` returns the same thing as
  a pointer.
- Do not copy a `Set` after first use. Pass a `*Set` instead.
- A `Set` is not safe for concurrent use. If goroutines share a set and any of them modifies it,
  synchronize access yourself.
- `All` and `ToSlice` return elements in unspecified order. Sort the result when order matters.
- Do not modify a set while you iterate over it with `All`, or from inside a predicate passed to
  one of its methods.
- Methods copy elements by value. A set of pointers shares the values the pointers refer to.
- The package has no special handling for elements that are not equal to themselves, such as a
  floating-point NaN.

## API

### Construction

| Function | Returns |
|---|---|
| `New[K]()` | An empty set. |
| `Of(elements...)` | A set holding the arguments. Duplicates are stored once. |
| `FromSlice(keys)` | A set holding the slice's elements. It does not keep a reference to the slice. |
| `Collect(seq)` | A set holding the elements an `iter.Seq[K]` yields. |

### Set algebra

Each operation has a copying form, which returns a new set and leaves both inputs unchanged, and
an in-place form, which modifies the receiver.

| Operation | Copying | In place |
|---|---|---|
| Elements in either set | `Union` | `Update` |
| Elements in both sets | `Intersection` | `IntersectionUpdate` |
| Receiver's elements not in `other` | `Difference` | `DifferenceUpdate` |
| Elements in exactly one set | `Disjunction` | `DisjunctionUpdate` |
| Elements that satisfy a predicate | `Filter` | `RemoveFunc` removes them |

`Disjunction` is the symmetric difference. `IntersectionUpdate` is Java's `retainAll`, and
`RemoveFunc` is Java's `removeIf`. Passing the receiver to `DisjunctionUpdate` empties it.

### Adding and removing

| Method | Effect |
|---|---|
| `Add(element)` | Adds one element. |
| `AddAll(seq)` | Adds every element an iterator yields. |
| `Remove(element)` | Removes one element. Removing an absent element does nothing. |
| `RemoveAll(seq)` | Removes every element an iterator yields. `s.RemoveAll(s.All())` is safe. |
| `Clear()` | Removes every element and releases the old storage. |

### Queries

| Method | Reports |
|---|---|
| `Contains(element)` | Whether the element is in the set. |
| `ContainsAll(seq)` | Whether every element an iterator yields is in the set. |
| `ContainsFunc(predicate)` | Whether any element satisfies the predicate. |
| `Equals(other)` | Whether both sets have the same elements. |
| `IsSubset(other)` | Whether every element of the receiver is in `other`. |
| `IsSuperset(other)` | Whether every element of `other` is in the receiver. |
| `IsDisjoint(other)` | Whether the sets share no element. |
| `IsEmpty()`, `Len()` | Whether the set is empty, and how many elements it holds. |

### Iteration and copies

| Method | Returns |
|---|---|
| `All()` | An `iter.Seq[K]` over the elements. |
| `ToSlice()` | A new slice of the elements. |
| `Clone()` | A new set with the same elements. |

`Predicate[K]` is an alias for `func(element K) bool`, so any function of that shape works.
