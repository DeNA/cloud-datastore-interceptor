package cache

import (
	"fmt"
)

// DelError wraps the cache delete operation error in an implementation of built-in Error.
type DelError struct {
	wrapped error
}

// Error implements error.
func (e DelError) Error() string {
	return fmt.Sprintf("cache del: %v", e.wrapped)
}

// Unwrap returns the wrapped error.
func (e DelError) Unwrap() error {
	return e.wrapped
}
