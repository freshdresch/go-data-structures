package buffer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesBuffer(t *testing.T) {
	bb := NewBytesBuffer(64)
	_, err := bb.WriteString("hello ")
	require.NoError(t, err)
	_, err = bb.Write([]byte("world"))
	require.NoError(t, err)
	require.NoError(t, bb.WriteByte('!'))

	assert.Equal(t, "hello world!", bb.String())

	p := make([]byte, 5)
	n, err := bb.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, "hello", string(p[:n]))
	assert.Equal(t, " world!", bb.String())
}

func TestBytesBuffer_PartialWrite(t *testing.T) {
	bb := NewBytesBuffer(4)

	n, err := bb.Write([]byte("abcdef"))
	assert.Equal(t, 4, n)
	assert.ErrorIs(t, err, ErrFull)
	assert.Equal(t, "abcd", bb.String())
}

func TestBytesBuffer_WriteStringWhenFull(t *testing.T) {
	bb := NewBytesBuffer(2)
	_, err := bb.WriteString("ab")
	require.NoError(t, err)

	n, err := bb.WriteString("c")
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrFull)
	assert.Equal(t, "ab", bb.String())
}

func TestBytesBuffer_WriteByteWhenFull(t *testing.T) {
	bb := NewBytesBuffer(1)
	require.NoError(t, bb.WriteByte('a'))

	err := bb.WriteByte('b')
	assert.ErrorIs(t, err, ErrFull)
	assert.Equal(t, "a", bb.String())
}

func TestBytesBuffer_ReadEmpty(t *testing.T) {
	bb := NewBytesBuffer(4)

	p := make([]byte, 2)
	n, err := bb.Read(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestBytesBuffer_ReadEmptySlice(t *testing.T) {
	bb := NewBytesBuffer(4)
	_, err := bb.WriteString("ab")
	require.NoError(t, err)

	n, err := bb.Read(nil)
	assert.Equal(t, 0, n)
	assert.NoError(t, err)
	assert.Equal(t, "ab", bb.String())
}

func TestBytesBuffer_Bytes(t *testing.T) {
	bb := NewBytesBuffer(4)
	_, err := bb.WriteString("hi")
	require.NoError(t, err)

	assert.Equal(t, []byte("hi"), bb.Bytes())
}
