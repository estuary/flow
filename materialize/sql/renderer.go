package sql

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

// Renderer is used for naming things inside of SQL. It can be used for fields or values to
// handle sanitization and quoting.
type Renderer struct {
	// If set, will sanitize field before rendering by passing it to this function
	sanitizer func(string) string
	// If set, will wrap value after optionally checking SkipWrapper
	wrapper func(string) string
	// If set, will check *sanitized* value to see if it should wrap. If unset, always wraps.
	skipWrapper func(string) bool
}

var (
	// DefaultUnwrappedIdentifiers is a SkipWrapper function that checks for identifiers that 1
	// typically do not need wrapping.
	DefaultUnwrappedIdentifiers = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`).MatchString

	// DefaultQuoteSanitizer used for sanitizing fields in SQL.
	DefaultQuoteSanitizer = strings.NewReplacer("'", "''").Replace
)

// NewRenderer returns a configured renderer instance.
func NewRenderer(sanitizer func(string) string, wrapper func(string) string, skipWrapper func(string) bool) *Renderer {
	return &Renderer{
		sanitizer:   sanitizer,
		wrapper:     wrapper,
		skipWrapper: skipWrapper,
	}
}

// Write takes a writer and renders text based on it's configuration.
func (r *Renderer) Write(w io.Writer, text string) (int, error) {
	if r == nil {
		return w.Write([]byte(text))
	}
	if r.sanitizer != nil {
		text = r.sanitizer(text)
	}
	if (r.skipWrapper != nil && r.skipWrapper(text)) || r.wrapper == nil {
		return w.Write([]byte(text))
	}
	return w.Write([]byte(r.wrapper(text)))
}

// Render takes a string and renders text based on it's configuration.
func (r *Renderer) Render(text string) string {
	var b strings.Builder
	_, _ = r.Write(&b, text)
	return b.String()
}

// Sanitize uses a SanitizerFunc or returns the original string if it's nil.
func (r *Renderer) Sanitize(text string) string {
	if r.sanitizer == nil {
		return text
	}
	return r.sanitizer(text)
}

// Wrap uses a WrapperFunc or returns the original string if it's nil.
func (r *Renderer) Wrap(text string) string {
	if (r.skipWrapper != nil && r.skipWrapper(text)) || r.wrapper == nil {
		return text
	}
	return r.wrapper(text)
}

// TokenPair is a generic way of representing strings that can be used to surround some text for
// quoting and commenting.
type TokenPair struct {
	Left  string
	Right string
}

// Wrap returns the given string surrounded by the strings in this TokenPair.
func (p *TokenPair) Wrap(text string) string {
	if p == nil {
		return text
	}
	return p.Left + text + p.Right
}

// Write takes an io.Writer and writes out the wrapped text.
// This function is leveraged for writing comments.
func (p *TokenPair) Write(w io.Writer, text string) (int, error) {
	if p == nil {
		return w.Write([]byte(text))
	} else {
		total, err := w.Write([]byte(p.Left))
		if err != nil {
			return total, err
		}
		next, err := w.Write([]byte(text))
		if err != nil {
			return total + next, err
		}
		total += next
		next, err = w.Write([]byte(p.Right))
		if err != nil {
			return total + next, err
		}
		total += next

		return total, nil
	}
}

// NewTokenPair returns a TokenPair with the left and right tokens specified.
func NewTokenPair(l, r string) *TokenPair {
	return &TokenPair{
		Left:  l,
		Right: r,
	}
}

// DoubleQuotes returns a wrapper function with a single double quote character
// on the both the Left and the Right.
func DoubleQuotesWrapper() func(text string) string {
	return (&TokenPair{
		Left:  "\"",
		Right: "\"",
	}).Wrap
}

// SingleQuotesWrapper returns a wrapper function with one single quote character
// on the both the Left and the Right.
func SingleQuotesWrapper() func(text string) string {
	return (&TokenPair{
		Left:  "'",
		Right: "'",
	}).Wrap
}

// Backticks returns a wrapper function with a single backtick character on the
// both the Left and the Right.
func BackticksWrapper() func(text string) string {
	return (&TokenPair{
		Left:  "`",
		Right: "`",
	}).Wrap
}

// CommentRenderer is used to render comments in SQL.
type CommentRenderer struct {
	// Linewise determines whether to render line or block comments. If it is true, then each line
	// of comment text will be wrapped separately. If false, then the entire multi-line block of
	// comment text will be wrapped once.
	Linewise bool
	// Wrap holds the strings that will bound the beginning and end of the comment.
	Wrap *TokenPair
}

// Write renders a comment based on the rules.
func (cr *CommentRenderer) Write(w io.Writer, text string, indent string) (int, error) {

	var scanner = bufio.NewScanner(strings.NewReader(text))

	// Inline function to handle writes and accumulate total bytes written.
	var total int
	var write = func(text string) error {
		n, err := w.Write([]byte(text))
		total += n
		return err
	}

	// Each line needs wrapped in a comment.
	if cr.Linewise {
		var first = true
		for scanner.Scan() {
			if !first {
				// Write newline and indent.
				if err := write("\n"); err != nil {
					return total, err
				}
				if err := write(indent); err != nil {
					return total, err
				}
			}
			first = false
			if err := write(cr.Wrap.Left); err != nil {
				return total, err
			}
			if err := write(scanner.Text()); err != nil {
				return total, err
			}
			if err := write(cr.Wrap.Right); err != nil {
				return total, err
			}
		}

	} else {
		// The whole comment is wrapped once.
		if err := write(cr.Wrap.Left); err != nil {
			return total, err
		}
		var first = true
		for scanner.Scan() {
			if !first {
				// Write newline and indent.
				if err := write("\n"); err != nil {
					return total, err
				}
				if err := write(indent); err != nil {
					return total, err
				}
			}
			first = false
			if err := write(scanner.Text()); err != nil {
				return total, err
			}
		}
		if err := write(cr.Wrap.Right); err != nil {
			return total, err
		}
	}

	// Comments always end with a newline.
	if err := write("\n"); err != nil {
		return total, err
	}

	return total, nil

}

// Render takes a string and renders it as a comment based on it's configuration.
func (cr *CommentRenderer) Render(text string) string {
	var b strings.Builder
	_, _ = cr.Write(&b, text, "")
	return b.String()
}

// LineCommentRenderer returns a per line comment valid for standard SQL.
func LineCommentRenderer() *CommentRenderer {
	return &CommentRenderer{
		Linewise: true,
		Wrap: &TokenPair{
			Left:  "-- ",
			Right: "",
		},
	}
}
