// Package ring implements a thread-safe circular (ring) buffer.
package ring

// TODO: Move to some common external package.

// Buffer is a thread-safe circular buffer.
type Buffer[T comparable] struct {
	data       []T
	capacity   int
	cursor     int
	full       bool
	size       int
	addedCount int64
}

// New creates a new ring buffer with the given capacity.
func New[T comparable](capacity int) *Buffer[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &Buffer[T]{
		data:     make([]T, capacity),
		capacity: capacity,
	}
}

// Capacity returns the capacity of the buffer.
func (b *Buffer[T]) Capacity() int {
	return b.capacity
}

// Size returns the current number of elements in the buffer.
func (b *Buffer[T]) Size() int {
	return b.size
}

// IsFull returns true if the buffer is at capacity.
func (b *Buffer[T]) IsFull() bool {
	return b.full
}

// Push adds an element to the buffer, overwriting the oldest element if full.
func (b *Buffer[T]) Push(value T) {
	if b.capacity == 0 {
		return
	}

	b.data[b.cursor] = value
	b.cursor = (b.cursor + 1) % b.capacity

	if b.full {
		// Buffer was full, so we overwrote the oldest element
		// Size stays at capacity
	} else {
		b.size++
		if b.size == b.capacity {
			b.full = true
		}
	}

	b.addedCount++
}

// PopOldest removes and returns the oldest element.
// Returns false if the buffer is empty.
func (b *Buffer[T]) PopOldest() (T, bool) {
	if b.size == 0 {
		var zero T
		return zero, false
	}

	// The oldest element is at position (cursor - size + capacity) % capacity
	oldestIndex := (b.cursor - b.size + b.capacity) % b.capacity
	value := b.data[oldestIndex]
	b.data[oldestIndex] = *new(T) // Zero out the slot
	b.size--
	b.full = false

	return value, true
}

// Get returns the element at the given index (0 = oldest).
// Index must be in range [0, size).
func (b *Buffer[T]) Get(index int) (T, bool) {
	if index < 0 || index >= b.size {
		var zero T
		return zero, false
	}

	oldestIndex := (b.cursor - b.size + b.capacity) % b.capacity
	actualIndex := (oldestIndex + index) % b.capacity
	return b.data[actualIndex], true
}

// Set updates the element at the given index (0 = oldest).
// Returns false if index is out of range.
func (b *Buffer[T]) Set(index int, value T) bool {
	if index < 0 || index >= b.size {
		return false
	}

	oldestIndex := (b.cursor - b.size + b.capacity) % b.capacity
	actualIndex := (oldestIndex + index) % b.capacity
	b.data[actualIndex] = value
	return true
}

// Slice returns a view of all elements in the buffer (oldest to newest).
// Returns nil if buffer is empty.
func (b *Buffer[T]) Slice() []T {
	if b.size == 0 {
		return nil
	}

	// We need to return elements in order from oldest to newest
	oldestIndex := (b.cursor - b.size + b.capacity) % b.capacity
	result := make([]T, b.size)
	for i := 0; i < b.size; i++ {
		idx := (oldestIndex + i) % b.capacity
		result[i] = b.data[idx]
	}
	return result
}

// TotalAdded returns the total number of elements ever added to the buffer.
func (b *Buffer[T]) TotalAdded() int64 {
	return b.addedCount
}

// Clear removes all elements from the buffer.
func (b *Buffer[T]) Clear() {
	for i := 0; i < b.capacity; i++ {
		b.data[i] = *new(T)
	}
	b.cursor = 0
	b.full = false
	b.size = 0
	b.addedCount = 0
}
