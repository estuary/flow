package protocol

func GetProjectionByField(field string, projections []*Projection) *Projection {
	for _, proj := range projections {
		if proj.Field == field {
			return proj
		}
	}
	return nil
}
