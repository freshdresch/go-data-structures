package buffer

import "sync"

// Pool hands out reusable Buffers that all have the same capacity. The zero value Pool is not
// usable. Create one with NewPool.
type Pool[T any] struct {
	pool      sync.Pool
	capacity  int
	zeroOnPut bool // whether Put wipes every element the released buffer ever held
}

// NewPool returns a Pool of Buffers with the given capacity. If zeroOnPut is true, Put wipes
// every element each released buffer ever held, including ones already removed. It panics if
// capacity is negative.
func NewPool[T any](capacity int, zeroOnPut bool) *Pool[T] {
	if capacity < 0 {
		panic("buffer: negative capacity")
	}
	return &Pool[T]{
		capacity:  capacity,
		zeroOnPut: zeroOnPut,
		pool: sync.Pool{
			New: func() any {
				return NewBuffer[T](capacity)
			},
		},
	}
}

// Get returns a buffer with Len() == 0 and Cap() equal to the pool's capacity. It may allocate a
// new buffer. Without zeroOnPut, the backing array may hold stale values that Grow exposes.
func (p *Pool[T]) Get() *Buffer[T] {
	return p.pool.Get().(*Buffer[T])
}

// Put resets released and may return it to the pool for reuse. When the pool was created with
// zeroOnPut, it also wipes every element released ever held, including ones already removed. A
// buffer whose Cap() differs from the pool's capacity is reset but not kept, so Get always
// returns a buffer of the pool's capacity. The caller must not use released after Put, because
// another goroutine may get it next. Put(nil) does nothing.
func (p *Pool[T]) Put(released *Buffer[T]) {
	if released == nil {
		return
	}

	if p.zeroOnPut {
		released.resetAndZeroAll()
	} else {
		released.Reset()
	}

	if p.accepts(released) {
		p.pool.Put(released)
	}
}

// accepts reports whether released has the pool's capacity.
func (p *Pool[T]) accepts(released *Buffer[T]) bool {
	return released.Cap() == p.capacity
}
