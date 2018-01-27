package recoverylog

import (
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var (
	ErrChecksumMismatch = fmt.Errorf("checksum mismatch")
	ErrFnodeNotTracked  = fmt.Errorf("fnode not tracked")
	ErrLinkExists       = fmt.Errorf("link exists")
	ErrNoSuchLink       = fmt.Errorf("fnode has no such link")
	ErrNotHinted        = fmt.Errorf("op recorder is not hinted")
	ErrPropertyExists   = fmt.Errorf("property exists")
	ErrWrongSeqNo       = fmt.Errorf("wrong sequence number")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// An Fnode is an identifier which represents a file across its renames, links,
// and unlinks within a file-system. When a file is created, it's assigned an
// Fnode value equal to the RecordedOp.SeqNo which created it.
type Fnode int64

// Processes which create RecordedOps are assigned a unique Author ID:
// a random, unique identifier which represents the process. It's used by FSM to
// allow for reconcilliation of divergent histories in the recovery log, through
// hints as to which Authors produced which Segments in the final history.
type Author uint32

type FnodeState struct {
	// Active current paths of this Fnode.
	Links map[string]struct{}
	// Ordered log Segments which contain Fnode operations.
	Segments []Segment
}

// FSM implements a finite state machine over RecordedOp. In particular FSM
// applies RecordedOp in order, verifying the SeqNo and Checksum of each
// operation. This ensures that only operations which are linear and
// consistent are applied.
type FSM struct {
	// Log, and maximum offset progress of this FSM. Note that FSM does not
	// maintain the LogMark.Offset field itself (it expects the caller too).
	LogMark journal.Mark

	// Expected sequence number and checksum of next operation.
	NextSeqNo    int64
	NextChecksum uint32

	// Target paths and contents of small files which are managed outside of
	// regular Fnode tracking. Property updates are triggered upon rename of
	// a tracked Fnode to a well-known property file path.
	//
	// Propertes paths must be "sinks" which:
	//  * Are never directly written to.
	//  * Are never renamed or linked from.
	//  * Have exactly one hard-link (eg, only "rename" to the property path is
	//    supported; "link" is not as it would introduce a second hard-link).
	Properties map[string]string

	// Maps from Fnode to current state of the node.
	LiveNodes map[Fnode]*FnodeState
	// Indexes current target paths of LiveNodes.
	Links map[string]Fnode

	// Ordered, non-overlapping segments of log to process.
	hintedSegments []Segment
	// Ordered Fnodes which are still live at |hintedSegments| completion.
	hintedFnodes []Fnode
}

func NewFSM(hints FSMHints) (*FSM, error) {
	var fsm = &FSM{
		LogMark:      journal.NewMark(hints.Log, 0),
		NextSeqNo:    1,
		NextChecksum: 0,
		Properties:   make(map[string]string),
		LiveNodes:    make(map[Fnode]*FnodeState),
		Links:        make(map[string]Fnode),
	}

	// Flatten all hinted LiveNodes Segments into single |set|.
	var set SegmentSet
	for i, n := range hints.LiveNodes {
		if i != 0 && fsm.hintedFnodes[i-1] >= n.Fnode {
			return nil, fmt.Errorf("invalid hint fnode ordering")
		}
		fsm.hintedFnodes = append(fsm.hintedFnodes, n.Fnode)

		for _, s := range n.Segments {
			if err := set.Add(s); err != nil {
				return nil, err
			}
		}
	}
	if len(set) != 0 {
		fsm.NextSeqNo, fsm.NextChecksum = set[0].FirstSeqNo, set[0].FirstChecksum
		fsm.hintedSegments = []Segment(set)
	}

	// Flatten hinted properties into |fsm|.
	for _, p := range hints.Properties {
		fsm.Properties[p.Path] = p.Content
	}
	return fsm, nil
}

func (m *FSM) Apply(op *RecordedOp, frame []byte) error {
	if op.SeqNo != m.NextSeqNo {
		return ErrWrongSeqNo
	} else if op.Checksum != m.NextChecksum {
		return ErrChecksumMismatch
	}

	// If hints remain, ensure that op.Author is hinted for this op.SeqNo.
	if len(m.hintedSegments) != 0 && m.hintedSegments[0].Author != op.Author {
		// This is a consistent operation, but written by a non-hinted Author
		// for this SeqNo: the operation represents a (likely dead) branch in
		// recovery-log history relative to the FSMHints we're re-building.
		return ErrNotHinted
	}

	// Note apply*() functions do not modify FSM state if they return an error.
	var err error
	if op.Create != nil {
		err = m.applyCreate(op)
	} else if op.Link != nil {
		err = m.applyLink(op)
	} else if op.Unlink != nil {
		err = m.applyUnlink(op)
	} else if op.Write != nil {
		err = m.applyWrite(op)
	} else if op.Property != nil {
		err = m.applyProperty(op.Property)
	}

	if err != nil && err != ErrFnodeNotTracked {
		// No state transition (or FSM mutation) occurred.
		return err
	}

	// Step the FSM to the next state.
	m.NextSeqNo += 1
	m.NextChecksum = crc32.Update(m.NextChecksum, crcTable, frame)

	// If we've exhausted the current hinted Segment, pop and skip to the next.
	if len(m.hintedSegments) != 0 && m.hintedSegments[0].LastSeqNo < m.NextSeqNo {
		m.hintedSegments = m.hintedSegments[1:]

		if len(m.hintedSegments) != 0 {
			m.NextSeqNo = m.hintedSegments[0].FirstSeqNo
			m.NextChecksum = m.hintedSegments[0].FirstChecksum
		}
	}
	return err
}

func (m *FSM) applyCreate(op *RecordedOp) error {
	if _, ok := m.Links[op.Create.Path]; ok {
		return ErrLinkExists
	} else if _, ok := m.Properties[op.Create.Path]; ok {
		return ErrPropertyExists
	}
	// Assigned fnode ID is the SeqNo of the current operation.
	fnode := Fnode(op.SeqNo)

	// Determine whether |fnode| is hinted.
	if len(m.hintedFnodes) != 0 {
		if m.hintedFnodes[0] != fnode {
			return ErrFnodeNotTracked
		}
		m.hintedFnodes = m.hintedFnodes[1:] // Pop hint.
	}

	node := &FnodeState{Links: map[string]struct{}{op.Create.Path: {}}}
	m.extendSegments(&node.Segments, op)

	m.LiveNodes[fnode] = node
	m.Links[op.Create.Path] = fnode

	return nil
}

func (m *FSM) applyLink(op *RecordedOp) error {
	if _, ok := m.Links[op.Link.Path]; ok {
		return ErrLinkExists
	} else if _, ok := m.Properties[op.Link.Path]; ok {
		return ErrPropertyExists
	}
	node, ok := m.LiveNodes[op.Link.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	}

	node.Links[op.Link.Path] = struct{}{}
	m.Links[op.Link.Path] = op.Link.Fnode
	m.extendSegments(&node.Segments, op)

	return nil
}

func (m *FSM) applyUnlink(op *RecordedOp) error {
	node, ok := m.LiveNodes[op.Unlink.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	} else if _, ok = node.Links[op.Unlink.Path]; !ok {
		return ErrNoSuchLink
	}

	delete(m.Links, op.Unlink.Path)
	delete(node.Links, op.Unlink.Path)
	m.extendSegments(&node.Segments, op)

	if len(node.Links) == 0 {
		// Fnode is no longer live (all links are removed).
		delete(m.LiveNodes, op.Unlink.Fnode)
	}

	return nil
}

func (m *FSM) applyWrite(op *RecordedOp) error {
	node, ok := m.LiveNodes[op.Write.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	}
	m.extendSegments(&node.Segments, op)

	return nil
}

func (m *FSM) applyProperty(op *Property) error {
	if _, ok := m.Links[op.Path]; ok {
		return ErrLinkExists
	} else if content, ok := m.Properties[op.Path]; ok && content != op.Content {
		return ErrPropertyExists
	}
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties[op.Path] = op.Content
	return nil
}

// BuildHints constructs FSMHints which enable a future FSM to rebuild this FSM's state.
func (m *FSM) BuildHints() FSMHints {
	var hints = FSMHints{
		Log: m.LogMark.Journal,
	}

	// Flatten LiveNodes into FnodeSegments.
	for fnode, state := range m.LiveNodes {
		hints.LiveNodes = append(hints.LiveNodes, FnodeSegments{fnode, state.Segments})
	}
	// Order LiveNodes on ascending Fnode ID, which is also the order LiveNodes will appear in the log.
	sort.Slice(hints.LiveNodes, func(i, j int) bool {
		return hints.LiveNodes[i].Fnode < hints.LiveNodes[j].Fnode
	})

	// Flatten properties.
	for path, content := range m.Properties {
		hints.Properties = append(hints.Properties, Property{Path: path, Content: content})
	}
	return hints
}

func (m *FSM) hasRemainingHints() bool {
	return len(m.hintedSegments) != 0 || len(m.hintedFnodes) != 0
}

func (m *FSM) extendSegments(s *[]Segment, op *RecordedOp) {
	if l := len(*s); l != 0 && (*s)[l-1].Author == op.Author {
		(*s)[l-1].LastSeqNo = op.SeqNo
	} else {
		*s = append(*s, Segment{
			Author:        op.Author,
			FirstChecksum: op.Checksum,
			FirstOffset:   m.LogMark.Offset,
			FirstSeqNo:    op.SeqNo,
			LastSeqNo:     op.SeqNo,
		})
	}
}
