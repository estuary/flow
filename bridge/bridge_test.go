package bridge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFoo(t *testing.T) {
	var m, err = NewMessage("some content", 1000)
	assert.Nil(t, err)

	assert.Equal(t, m.Length(), 1012)
	m.Extend()
	assert.Equal(t, m.Length(), 1018)

	m.Free()
}
