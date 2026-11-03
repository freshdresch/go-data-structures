# xmaps

`xmaps` provides generic map functions that complement the standard `maps` package. Each function
returns a new slice or map and leaves its inputs unchanged.

```sh
go get github.com/freshdresch/go-data-structures/xmaps
```

## Example

```go
package main

import (
	"fmt"
	"log"

	"github.com/freshdresch/go-data-structures/xmaps"
)

func main() {
	defaults := map[string]int{"workers": 2, "retries": 3}
	overrides := map[string]int{"workers": 4}

	config := xmaps.Merge(defaults, overrides)
	fmt.Println(config)

	byValue, err := xmaps.Invert(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(byValue[4])
}
```

Output:

```text
map[retries:3 workers:4]
workers
```

## API

| Function | Returns |
|---|---|
| `Keys(source)` | A slice of the keys. It is like `slices.Collect(maps.Keys(source))`, but allocates the full length up front. |
| `Values(source)` | A slice of the values, allocated the same way as `Keys`. |
| `Invert(source)` | A map with keys and values swapped. If two keys share a value, it returns a nil map and an error that wraps `ErrDuplicateValue`. |
| `Merge(sources...)` | A map with the entries of every source. When a key repeats, the later map wins. It returns the input's map type. |

`Keys` and `Values` return elements in unspecified order, as map iteration does. Sort the result
when order matters.

Check for an `Invert` failure with `errors.Is(err, xmaps.ErrDuplicateValue)`. When several values
repeat, map iteration order decides which one the error names.
