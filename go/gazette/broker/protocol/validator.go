package protocol

import (
	"fmt"
	"path"
	"strings"
	"unicode"
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

// ValidateToken ensures the string is of length [min, max] and consists
// only of runes drawn from a restricted set: unicode.Letter and unicode.Digit
// character classes, and passed allowed symbols.
// Tokens are simple strings which represent things like member zones, suffixes,
// label names, and values. They generally do not allow spaces.
func ValidateToken(n, symbols string, min, max int) error {
	if l := len(n); l < min || l > max {
		return NewValidationError("invalid length (%d; expected %d <= length <= %d)", l, min, max)
	}
	for _, r := range n {
		// Note that any character with ordinal value less than or equal to '#' (35),
		// which is the allocator KeySpace separator, cannot be included in this alphabet.
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			continue
		} else if !strings.ContainsRune(symbols, r) {
			return NewValidationError("not a valid token (%s)", n)
		}
	}
	return nil
}

// ValidatePathComponent ensures the string is of length [min, max], that it is
// a "clean" path (as defined by path.Clean), is non-rooted, and consists only
// of characters drawn from pathSymbols.
func ValidatePathComponent(n string, min, max int) error {
	if err := ValidateToken(n, pathSymbols, min, max); err != nil {
		return err
	} else if n != "" && path.Clean(n) != n {
		return NewValidationError("must be a clean path (%s)", n)
	} else if n != "" && n[0] == '/' {
		return NewValidationError("cannot begin with '/' (%s)", n)
	}
	return nil
}

const (
	// TokenSymbols is allowed runes of "tokens",
	// Note that any character with ordinal value less than or equal to '#' (35),
	// which is the allocator KeySpace separator, must not be included in this alphabet.
	// The alphabet leads with '-' to facilitate escaping in |reToken|.
	TokenSymbols = "-_+/."
	// pathSymbols is allowed runes of strings which form path components.
	// It extends TokenSymbols with the '=' and '%' runes.
	pathSymbols = TokenSymbols + "=%"
	// querySymbols is allowed runes of strings which form query components.
	querySymbols = "-_+.&=%"
)
