package recoverylog

// Validate validates a FSMHints instance.
// TODO: consider additional validation.
func (h *FSMHints) Validate() error {
	for _, node := range h.LiveNodes {
		for _, segment := range node.Segments {
			if err := segment.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}
