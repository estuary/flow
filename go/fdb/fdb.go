/*
 * fdb.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FoundationDB Go API

package fdb

// NOTE(johnny): I've stripped this source file to the minimal bits
// needed to support the `tuple` package.

import (
	"bytes"
	"fmt"
)

// A KeyConvertible can be converted to a FoundationDB Key. All functions in the
// FoundationDB API that address a specific key accept a KeyConvertible.
type KeyConvertible interface {
	FDBKey() Key
}

// Key represents a FoundationDB key, a lexicographically-ordered sequence of
// bytes. Key implements the KeyConvertible interface.
type Key []byte

// FDBKey allows Key to (trivially) satisfy the KeyConvertible interface.
func (k Key) FDBKey() Key {
	return k
}

// String describes the key as a human readable string.
func (k Key) String() string {
	return Printable(k)
}

// Printable returns a human readable version of a byte array. The bytes that correspond with
// ASCII printable characters [32-127) are passed through. Other bytes are
// replaced with \x followed by a two character zero-padded hex code for byte.
func Printable(d []byte) string {
	buf := new(bytes.Buffer)
	for _, b := range d {
		if b >= 32 && b < 127 && b != '\\' {
			buf.WriteByte(b)
			continue
		}
		if b == '\\' {
			buf.WriteString("\\\\")
			continue
		}
		buf.WriteString(fmt.Sprintf("\\x%02x", b))
	}
	return buf.String()
}

// Returns the API version that has been selected through APIVersion
// or MustAPIVersion. If the version has already been selected, then
// the first value returned is the API version and the error is
// nil. If the API version has not yet been set, then the error
// will be non-nil.
func GetAPIVersion() (int, error) {
	return 0, fmt.Errorf("API Version is not implemented")
}
