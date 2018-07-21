package protocol

import (
	epb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

// Validate returns an error if the Header is not well-formed.
func (m Header) Validate() error {
	if err := m.ProcessId.Validate(); err != nil {
		return ExtendContext(err, "ProcessId")
	} else if err := m.Route.Validate(); err != nil {
		return ExtendContext(err, "Route")
	} else if err := m.Etcd.Validate(); err != nil {
		return ExtendContext(err, "Etcd")
	}
	return nil
}

// Validate returns an error if the Header_Etcd is not well-formed.
func (m Header_Etcd) Validate() error {
	if m.ClusterId == 0 {
		return NewValidationError("invalid ClusterId (expected != 0)")
	} else if m.MemberId == 0 {
		return NewValidationError("invalid MemberId (expected != 0)")
	} else if m.Revision <= 0 {
		return NewValidationError("invalid Revision (%d; expected 0 < revision)", m.Revision)
	} else if m.RaftTerm == 0 {
		return NewValidationError("invalid RaftTerm (expected != 0)")
	}
	return nil
}

// FromEtcdResponseHeader converts an etcd ResponseHeader to an equivalent Header_Etcd.
func FromEtcdResponseHeader(h epb.ResponseHeader) Header_Etcd {
	return Header_Etcd{
		ClusterId: h.ClusterId,
		MemberId:  h.MemberId,
		Revision:  h.Revision,
		RaftTerm:  h.RaftTerm,
	}
}
