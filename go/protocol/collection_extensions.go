package protocol

// GetProjectionByField finds the projection with the given field name, or nil if one does not exist
func GetProjectionByField(field string, projections []*Projection) *Projection {
	for _, proj := range projections {
		if proj.Field == field {
			return proj
		}
	}
	return nil
}
