package protocol

import (
	"net/url"
	"strings"
	"time"
)

// RoutedJournalClient composes a JournalClient and DispatchRouter.
type RoutedJournalClient interface {
	JournalClient
	DispatchRouter
}

// NewRoutedJournalClient composes a JournalClient and DispatchRouter.
func NewRoutedJournalClient(jc JournalClient, dr DispatchRouter) RoutedJournalClient {
	return struct {
		JournalClient
		DispatchRouter
	}{
		JournalClient:  jc,
		DispatchRouter: dr,
	}
}

// Validate returns an error if the ReadRequest is not well-formed.
func (m *ReadRequest) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return ExtendContext(err, "Header")
		}
	}
	if err := m.Journal.Validate(); err != nil {
		return ExtendContext(err, "Journal")
	} else if m.Offset < -1 {
		return NewValidationError("invalid Offset (%d; expected -1 <= Offset <= MaxInt64)", m.Offset)
	}

	// Block, DoNotProxy, and MetadataOnly (each type bool) require no extra validation.

	return nil
}

// Validate returns an error if the ReadResponse is not well-formed.
func (m *ReadResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	}

	if m.Content != nil {
		// Require that no other fields are set.
		if m.Status != Status_OK {
			return NewValidationError("unexpected Status with Content (%v)", m.Status)
		} else if m.Header != nil {
			return NewValidationError("unexpected Header with Content (%s)", m.Header)
		} else if m.WriteHead != 0 {
			return NewValidationError("unexpected WriteHead with Content (%d)", m.WriteHead)
		} else if m.Fragment != nil {
			return NewValidationError("unexpected Fragment with Content (%s)", m.Fragment)
		} else if m.FragmentUrl != "" {
			return NewValidationError("unexpected FragmentUrl with Content (%s)", m.FragmentUrl)
		}
		return nil
	}

	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return ExtendContext(err, "Header")
		}
	}
	if m.WriteHead < 0 {
		return NewValidationError("invalid WriteHead (%d; expected >= 0)", m.WriteHead)
	}

	if m.Fragment != nil {
		if err := m.Fragment.Validate(); err != nil {
			return ExtendContext(err, "Fragment")
		} else if m.Offset < m.Fragment.Begin || m.Offset >= m.Fragment.End {
			return NewValidationError("invalid Offset (%d; expected %d <= offset < %d)",
				m.Offset, m.Fragment.Begin, m.Fragment.End)
		} else if m.WriteHead < m.Fragment.End {
			return NewValidationError("invalid WriteHead (%d; expected >= %d)",
				m.WriteHead, m.Fragment.End)
		}
		if m.FragmentUrl != "" {
			if _, err := url.Parse(m.FragmentUrl); err != nil {
				return ExtendContext(&ValidationError{Err: err}, "FragmentUrl")
			}
		}
	} else {
		if m.Status == Status_OK && m.Offset != 0 {
			return NewValidationError("unexpected Offset without Fragment or Content (%d)", m.Offset)
		} else if m.FragmentUrl != "" {
			return NewValidationError("unexpected FragmentUrl without Fragment (%s)", m.FragmentUrl)
		}
	}
	return nil
}

// Validate returns an error if the AppendRequest is not well-formed.
func (m *AppendRequest) Validate() error {
	if m.Journal != "" {
		if m.Header != nil {
			if err := m.Header.Validate(); err != nil {
				return ExtendContext(err, "Header")
			}
		}
		if err := m.Journal.Validate(); err != nil {
			return ExtendContext(err, "Journal")
		} else if m.Offset < 0 {
			return NewValidationError("invalid Offset (%d; expected >= 0)", m.Offset)
		} else if len(m.Content) != 0 {
			return NewValidationError("unexpected Content")
		}
	} else if m.Header != nil {
		return NewValidationError("unexpected Header")
	} else if m.DoNotProxy {
		return NewValidationError("unexpected DoNotProxy")
	} else if m.Offset != 0 {
		return NewValidationError("unexpected Offset")
	}
	return nil
}

// Validate returns an error if the AppendResponse is not well-formed.
func (m *AppendResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	} else if m.Header == nil {
		return NewValidationError("expected Header")
	} else if err = m.Header.Validate(); err != nil {
		return ExtendContext(err, "Header")
	} else if m.Status == Status_OK && m.Commit == nil {
		return NewValidationError("expected Commit")
	}
	if m.Commit != nil {
		if err := m.Commit.Validate(); err != nil {
			return ExtendContext(err, "Commit")
		}
	}
	return nil
}

// Validate returns an error if the ReplicateRequest is not well-formed.
func (m *ReplicateRequest) Validate() error {

	if m.Journal != "" {
		// This is an initial request.

		if err := m.Journal.Validate(); err != nil {
			return ExtendContext(err, "Journal")
		} else if m.Header == nil {
			return NewValidationError("expected Header with Journal")
		} else if err = m.Header.Validate(); err != nil {
			return ExtendContext(err, "Header")
		} else if m.Proposal == nil {
			return NewValidationError("expected Proposal with Journal")
		} else if err = m.Proposal.Validate(); err != nil {
			return ExtendContext(err, "Proposal")
		} else if m.Proposal.Journal != m.Journal {
			return NewValidationError("Journal and Proposal.Journal mismatch (%s vs %s)", m.Journal, m.Proposal.Journal)
		} else if m.Content != nil {
			return NewValidationError("unexpected Content with Journal (len %d)", len(m.Content))
		} else if m.ContentDelta != 0 {
			return NewValidationError("unexpected ContentDelta with Journal (%d)", m.ContentDelta)
		} else if m.Acknowledge == false {
			return NewValidationError("expected Acknowledge with Journal")
		}
		return nil
	}

	if m.Header != nil {
		return NewValidationError("unexpected Header without Journal (%s)", m.Header)
	}

	if m.Proposal != nil {
		if err := m.Proposal.Validate(); err != nil {
			return ExtendContext(err, "Proposal")
		} else if m.Content != nil {
			return NewValidationError("unexpected Content with Proposal (len %d)", len(m.Content))
		} else if m.ContentDelta != 0 {
			return NewValidationError("unexpected ContentDelta with Proposal (%d)", m.ContentDelta)
		}
		return nil
	}

	if m.Content == nil {
		return NewValidationError("expected Content or Proposal")
	}
	if m.Acknowledge {
		return NewValidationError("unexpected Acknowledge with Content")
	}
	if m.ContentDelta < 0 {
		return NewValidationError("invalid ContentDelta (%d; expected >= 0)", m.ContentDelta)
	}

	return nil
}

// Validate returns an error if the ReplicateResponse is not well-formed.
func (m *ReplicateResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	}

	if m.Status == Status_WRONG_ROUTE {
		if m.Header == nil {
			return NewValidationError("expected Header")
		} else if err := m.Header.Validate(); err != nil {
			return ExtendContext(err, "Header")
		}
	} else if m.Header != nil {
		return NewValidationError("unexpected Header (%s)", m.Header)
	}

	if m.Status == Status_FRAGMENT_MISMATCH {
		if m.Fragment == nil {
			return NewValidationError("expected Fragment")
		} else if err := m.Fragment.Validate(); err != nil {
			return ExtendContext(err, "Fragment")
		}
	} else if m.Fragment != nil {
		return NewValidationError("unexpected Fragment (%s)", m.Fragment)
	}

	return nil
}

func (m *ListRequest) Validate() error {
	if err := m.Selector.Validate(); err != nil {
		return ExtendContext(err, "Selector")
	}
	for _, v := range m.Selector.Include.ValuesOf("prefix") {
		if !strings.HasSuffix(v, "/") {
			return NewValidationError("Selector.Include.Labels[\"prefix\"]: expected trailing '/' (%+v)", v)
		}
	}
	for _, v := range m.Selector.Exclude.ValuesOf("prefix") {
		if !strings.HasSuffix(v, "/") {
			return NewValidationError("Selector.Exclude.Labels[\"prefix\"]: expected trailing '/' (%+v)", v)
		}
	}

	// PageLimit and PageToken require no extra validation.

	return nil
}

func (m *ListResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return ExtendContext(err, "Header")
	}
	for i, j := range m.Journals {
		if err := j.Validate(); err != nil {
			return ExtendContext(err, "Journals[%d]", i)
		}
	}

	// NextPageToken requires no extra validation.

	return nil
}

func (m *ListResponse_Journal) Validate() error {
	if err := m.Spec.Validate(); err != nil {
		return ExtendContext(err, "Spec")
	} else if m.ModRevision <= 0 {
		return NewValidationError("invalid ModRevision (%d; expected > 0)", m.ModRevision)
	} else if err = m.Route.Validate(); err != nil {
		return ExtendContext(err, "Route")
	}
	return nil
}

func (m *ApplyRequest) Validate() error {
	for i, u := range m.Changes {
		if err := u.Validate(); err != nil {
			return ExtendContext(err, "Changes[%d]", i)
		}
	}
	return nil
}

func (m *ApplyRequest_Change) Validate() error {
	if m.Upsert != nil {
		if m.Delete != "" {
			return NewValidationError("both Upsert and Delete are set (expected exactly one)")
		} else if err := m.Upsert.Validate(); err != nil {
			return ExtendContext(err, "Upsert")
		} else if m.ExpectModRevision < 0 {
			return NewValidationError("invalid ExpectModRevision (%d; expected >= 0)", m.ExpectModRevision)
		}
	} else if m.Delete != "" {
		if err := m.Delete.Validate(); err != nil {
			return ExtendContext(err, "Delete")
		} else if m.ExpectModRevision <= 0 {
			return NewValidationError("invalid ExpectModRevision (%d; expected > 0)", m.ExpectModRevision)
		}
	} else {
		return NewValidationError("neither Upsert nor Delete are set (expected exactly one)")
	}
	return nil
}

func (m *ApplyResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return ExtendContext(err, "Header")
	}
	return nil
}

func (m *FragmentsRequest) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return ExtendContext(err, "Header")
		}
	}
	if err := m.Journal.Validate(); err != nil {
		return ExtendContext(err, "Journal")
	}
	if m.EndModTime.Before(m.BeginModTime) {
		return NewValidationError("invalid EndModTime (%v must be after the %v)", m.EndModTime.In(time.UTC), m.BeginModTime.In(time.UTC))
	}
	if m.NextPageToken < 0 {
		return NewValidationError("invalid NextPageToken (%v; must be >= 0)", m.NextPageToken)
	}
	if m.PageLimit < 0 {
		return NewValidationError("invalid PageLimit (%v; must be >= 0)", m.PageLimit)
	}

	return nil
}

func (m *FragmentsResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return ExtendContext(err, "Header")
	}
	return nil
}

// Validate returns an error if the Status is not well-formed.
func (x Status) Validate() error {
	if _, ok := Status_name[int32(x)]; !ok {
		return NewValidationError("invalid status (%s)", x)
	}
	return nil
}
