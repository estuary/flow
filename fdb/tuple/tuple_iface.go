package tuple

// ToInterface maps a Tuple to a []interface{}.
// This is useful within interfaces that take []interface{} splats,
// like SQL drivers.
func (t Tuple) ToInterface() []interface{} {
	var m = make([]interface{}, len(t))
	for i, v := range t {
		m[i] = v
	}
	return m
}
