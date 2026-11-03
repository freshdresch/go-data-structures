# xslices

`xslices` provides generic slice functions that the standard `slices` package lacks, plus a few
aliases for standard functions under familiar names.

```sh
go get github.com/freshdresch/go-data-structures/xslices
```

## Example

```go
package main

import (
	"fmt"

	"github.com/freshdresch/go-data-structures/xslices"
)

func main() {
	values := xslices.Unique([]int{3, 1, 3, 2, 1})
	odd, even := xslices.Partition(values, func(value int) bool {
		return value%2 != 0
	})
	fmt.Println(values, odd, even)
	fmt.Println(xslices.IndicesOf([]string{"a", "b", "a"}, "a"))
}
```

Output:

```text
[3 1 2] [3 1] [2]
[0 2]
```

## API

These functions have no standard library equivalent. Each returns a new slice and leaves its input
unchanged. Functions that return elements return the input's slice type.

| Function | Returns |
|---|---|
| `Unique(inSlice)` | The distinct elements, in first-seen order. Unlike `slices.Compact`, it removes duplicates that are not adjacent. |
| `Reversed(inSlice)` | The elements in reverse order. |
| `Partition(inSlice, predicate)` | The elements that satisfy `predicate`, then the elements that do not, each in original order. |
| `LastIndexOf(inSlice, element)` | The index of the last occurrence, or -1. |
| `IndicesOf(inSlice, element)` | The indices of every occurrence, in increasing order. It returns an empty slice, not nil, when there are none. |

These functions call the standard function in the second column:

| Function | Calls |
|---|---|
| `Reverse(inSlice)` | `slices.Reverse`, which reverses in place. |
| `IndexOf(inSlice, element)` | `slices.Index` |
| `Contains(inSlice, element)` | `slices.Contains` |
