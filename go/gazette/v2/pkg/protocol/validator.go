package protocol

import (
	"fmt"
	"strings"
)

// Validator is a type able to validate itself. Validate inspects the type for
// syntactic or semantic issues, and returns a descriptive error if any
// violations are encountered. It is recommended that Validate return instances
// of ValidationError where possible, which enables tracking nested contexts.
type Validator interface {
	Validate() error
}

// ValidationError is an error implementation which captures its validation context.
type ValidationError struct {
	Context []string
	Err     error
}

// Error implements the error interface.
func (ve *ValidationError) Error() string {
	if len(ve.Context) != 0 {
		return strings.Join(ve.Context, ".") + ": " + ve.Err.Error()
	} else {
		return ve.Err.Error()
	}
}

// ExtendContext type-checks |err| to a *ValidationError, and if matched extends
// it with |context|. In all cases the value of |err| is returned.
func ExtendContext(err error, format string, args ...interface{}) error {
	if ve, ok := err.(*ValidationError); ok {
		ve.Context = append([]string{fmt.Sprintf(format, args...)}, ve.Context...)
	}
	return err
}

// NewValidationError parallels fmt.Errorf to returns a new ValidationError instance.
func NewValidationError(format string, args ...interface{}) error {
	return &ValidationError{Err: fmt.Errorf(format, args...)}
}

// ValidateToken ensures the string consists only of |tokenAlphabet| characters,
// and is of length |min| <= len(n) <= |max|.
func ValidateToken(n string, min, max int) error {
	if l := len(n); l < min || l > max {
		return NewValidationError("invalid length (%d; expected %d <= length <= %d)", l, min, max)
	} else if len(strings.Trim(n, tokenAlphabet)) != 0 {
		return NewValidationError("not a valid token (%s)", n)
	}
	return nil
}

const (
	// Note that any character with ordinal value less than or equal to '#' (35),
	// which is the allocator KeySpace separator, must not be included in this alphabet.
	// The alphabet leads with '-' to facilitate escaping in |reToken|.
	tokenAlphabet = "-_+/.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)
