// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import "sync"

// Pool manages reusable Buffers of a fixed capacity.
type Pool[T any] struct {
	pool  sync.Pool
	clear bool // whether to wipe the buffer contents before returning to the pool
}

// NewPool creates a pool of Buffers with the given capacity.
func NewPool[T any](bufCap int, clear bool) *Pool[T] {
	return &Pool[T]{
		clear: clear,
		pool: sync.Pool{
			New: func() any {
				return NewBuffer[T](bufCap)
			},
		},
	}
}

// Get retrieves a Buffer from the pool.
func (p *Pool[T]) Get() *Buffer[T] {
	return p.pool.Get().(*Buffer[T])
}

// Put returns a Buffer to the pool (reset before returning).
func (p *Pool[T]) Put(b *Buffer[T]) {
	if b == nil {
		return
	}

	if p.clear {
		b.ResetAndZero()
	} else {
		b.Reset()
	}
	p.pool.Put(b)
}
