package cloudstore

import "fmt"

// Properties gets values for keys, and allows a Filesytem to configure itself.
type Properties interface {
	Get(string) string
}

// EmptyProperties returns an empty set of properties, useful for callers who
// don't need to specify any additional connection parameters when initializing
// a FileSystem.
func EmptyProperties() Properties {
	return make(MapProperties)
}

// MapProperties is a simple implementation of Properties backed by an in-memory map.
type MapProperties map[string]string

// Get satisfies the |Properties| interface.
func (mp MapProperties) Get(key string) string {
	return mp[key]
}

// mergeProperties only works when both |a| and |b| are MapProperties. merges
// the values from |a| onto |b|.
func mergeProperties(a, b Properties) (Properties, error) {
	if src, ok := a.(MapProperties); !ok {
		return nil, fmt.Errorf("%v is not a MapProperties", a)
	} else if dest, ok := b.(MapProperties); !ok {
		return nil, fmt.Errorf("%v is not a MapProperties", b)
	} else {
		for k, v := range src {
			dest[k] = v
		}
		return dest, nil
	}
}
