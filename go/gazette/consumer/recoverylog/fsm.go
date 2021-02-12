package recoverylog

import (
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"sort"
	"strconv"

	pb "go.gazette.dev/core/broker/protocol"
)

var (
	ErrChecksumMismatch    = fmt.Errorf("checksum mismatch")
	ErrExpectedHintedFnode = fmt.Errorf("op is not create, and an Fnode was hinted")
	ErrFnodeNotTracked     = fmt.Errorf("fnode not tracked")
	ErrLinkExists          = fmt.Errorf("link exists")
	ErrNoSuchLink          = fmt.Errorf("fnode has no such link")
	ErrNotHintedAuthor     = fmt.Errorf("op author does not match the next hinted author")
	ErrPropertyExists      = fmt.Errorf("property exists")
	ErrWrongSeqNo          = fmt.Errorf("wrong sequence number")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// An Fnode is an identifier which represents a file across its renames, links,
// and un-links within a file-system. When a file is created, it's assigned an
// Fnode value equal to the RecordedOp.SeqNo which created it.
type Fnode int64

// Author is a random, unique ID which identifies a processes that creates RecordedOps.
// It's used by FSM to allow for reconciliation of divergent histories in the recovery
// log, through hints as to which Authors produced which Segments in the final history.
// Authors are also used for cooperative write-fencing between Player.InjectHandoff
// and Recorder.
type Author uint32

// NewRandomAuthor creates and returns a new, randomized Author in the range [1, math.MaxUint32].
func NewRandomAuthor() Author {
	if id, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32-1)); err != nil {
		panic(err.Error()) // |rand.Reader| must never error.
	} else {
		return Author(id.Int64()) + 1
	}
}

// Fence returns the journal register which fences journal appends for this Author.
func (a Author) Fence() *pb.LabelSet {
	return &pb.LabelSet{Labels: []pb.Label{{
		Name:  "author",
		Value: strconv.FormatUint(uint64(a), 32),
	}}}
}

// fnodeState is the state of an individual Fnode as tracked by FSM.
type fnodeState struct {
	// Links is the current set of filesystem paths (hard-links) of this Fnode.
	Links map[string]struct{}
	// Segments is the ordered set of log Segments containing the Fnode's operations.
	// It's tracked only for the production of FSMHints.
	Segments []Segment
}

// FSM implements a finite state machine over RecordedOp. In particular FSM
// applies RecordedOp in order, verifying the SeqNo and Checksum of each
// operation. This ensures that only operations which are linear and
// consistent are applied.
type FSM struct {
	// Expected sequence number and checksum of next operation.
	NextSeqNo    int64
	NextChecksum uint32

	// Target paths and contents of small files which are managed outside of
	// regular Fnode tracking. Property updates are triggered upon rename of
	// a tracked Fnode to a well-known property file path.
	//
	// Property paths must be "sinks" which:
	//  * Are never directly written to.
	//  * Are never renamed or linked from.
	//  * Have exactly one hard-link (eg, only "rename" to the property path is
	//    supported; "link" is not as it would introduce a second hard-link).
	Properties map[string]string

	// Maps from Fnode to current state of the node.
	LiveNodes map[Fnode]*fnodeState
	// Indexes current target paths of LiveNodes.
	Links map[string]Fnode

	// Ordered, non-overlapping segments of log to process.
	hintedSegments []Segment
	// Ordered Fnodes which are still live at |hintedSegments| completion.
	hintedFnodes []Fnode
}

// LiveLogSegments flattens hinted LiveNodes into an ordered list of Fnodes,
// and the set of recovery log Segments which fully contain them.
func (m FSMHints) LiveLogSegments() ([]Fnode, SegmentSet, error) {
	var fnodes []Fnode
	var set SegmentSet

	for i, n := range m.LiveNodes {
		if len(n.Segments) == 0 || Fnode(n.Segments[0].FirstSeqNo) != n.Fnode {
			return nil, nil, fmt.Errorf("expected Fnode to match Segment FirstSeqNo: %v", n)
		} else if i != 0 && fnodes[i-1] >= n.Fnode {
			return nil, nil, fmt.Errorf("expected monotonic Fnode ordering: %v vs %v", fnodes[i-1], n.Fnode)
		}
		fnodes = append(fnodes, n.Fnode)

		for _, segment := range n.Segments {
			// FSMHints defines zeroed Segment.Log as FSMHints.Log.
			if segment.Log == "" {
				segment.Log = m.Log
			}
			if err := set.Add(segment); err != nil {
				return nil, nil, err
			}
		}
	}
	return fnodes, set, nil
}

// NewFSM returns an FSM which is prepared to apply the provided |hints|.
func NewFSM(hints FSMHints) (*FSM, error) {
	if hints.Log == "" {
		return nil, fmt.Errorf("hinted log not provided")
	}

	var fnodes, set, err = hints.LiveLogSegments()
	if err != nil {
		return nil, err
	}

	var fsm = &FSM{
		NextSeqNo:    1,
		NextChecksum: 0,
		Properties:   make(map[string]string),
		LiveNodes:    make(map[Fnode]*fnodeState),
		Links:        make(map[string]Fnode),
		hintedFnodes: fnodes,
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

// Apply attempts to transition the FSMs state by |op| & |frame|. It either
// performs a transition, or returns an error detailing how the operation
// is not consistent with prior applied operations or hints. A state
// transition occurs if and only if the returned err is nil, with one
// exception: ErrFnodeNotTracked may be returned to indicate that the operation
// is consistent and a transition occurred, but that FSMHints also indicate the
// Fnode no longer exists at the point-in-time at which the FSMHints were created,
// and the caller may therefor want to skip corresponding local playback actions.
func (m *FSM) Apply(op *RecordedOp, frame []byte) error {
	// If hints remain, ensure that op.Author is the expected next operation author.
	if len(m.hintedSegments) != 0 && m.hintedSegments[0].Author != op.Author {
		// This may be a consistent operation, but is written by a non-hinted
		// Author for the next SeqNo of interest: the operation represents a
		// (likely dead) branch in recovery-log history relative to the
		// FSMHints we're re-building.
		return ErrNotHintedAuthor
	}

	if op.SeqNo != m.NextSeqNo {
		return ErrWrongSeqNo
	} else if op.Checksum != m.NextChecksum {
		return ErrChecksumMismatch
	}

	// Note apply*() functions do not modify FSM state if they return an error.
	var err error
	if op.Create != nil {
		err = m.applyCreate(op)
	} else if len(m.hintedFnodes) != 0 && int64(m.hintedFnodes[0]) == op.SeqNo {
		err = ErrExpectedHintedFnode
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
	m.NextSeqNo++
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
	var fnode = Fnode(op.SeqNo)

	// Determine whether |fnode| is hinted.
	if len(m.hintedFnodes) != 0 {
		if m.hintedFnodes[0] != fnode {
			return ErrFnodeNotTracked
		}
		m.hintedFnodes = m.hintedFnodes[1:] // Pop hint.
	}

	var node = &fnodeState{Links: map[string]struct{}{op.Create.Path: {}}}
	node.Segments = m.extendSegments(node.Segments, op)

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
	var node, ok = m.LiveNodes[op.Link.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	}

	node.Links[op.Link.Path] = struct{}{}
	m.Links[op.Link.Path] = op.Link.Fnode
	node.Segments = m.extendSegments(node.Segments, op)

	return nil
}

func (m *FSM) applyUnlink(op *RecordedOp) error {
	var node, ok = m.LiveNodes[op.Unlink.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	} else if _, ok = node.Links[op.Unlink.Path]; !ok {
		return ErrNoSuchLink
	}

	delete(m.Links, op.Unlink.Path)
	delete(node.Links, op.Unlink.Path)
	node.Segments = m.extendSegments(node.Segments, op)

	if len(node.Links) == 0 {
		// Fnode is no longer live (all links are removed).
		delete(m.LiveNodes, op.Unlink.Fnode)
	}

	return nil
}

func (m *FSM) applyWrite(op *RecordedOp) error {
	var node, ok = m.LiveNodes[op.Write.Fnode]
	if !ok {
		return ErrFnodeNotTracked
	}
	node.Segments = m.extendSegments(node.Segments, op)

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
func (m *FSM) BuildHints(log pb.Journal) FSMHints {
	var hints = FSMHints{Log: log}

	// Flatten LiveNodes into deep-copied FnodeSegments.
	for fnode, state := range m.LiveNodes {
		var segments = append([]Segment(nil), state.Segments...)

		// Remove explicit Segment.Logs which overlap with FSMHints.Log.
		for s := range segments {
			if segments[s].Log == hints.Log {
				segments[s].Log = ""
			}
		}

		hints.LiveNodes = append(hints.LiveNodes, FnodeSegments{
			Fnode:    fnode,
			Segments: segments,
		})
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

func (m *FSM) extendSegments(s []Segment, op *RecordedOp) []Segment {
	if l := len(s) - 1; l >= 0 && s[l].Author == op.Author && s[l].Log == op.Log {
		s[l].LastSeqNo = op.SeqNo
		s[l].LastOffset = op.LastOffset
		return s
	}
	return append(s, Segment{
		Author:        op.Author,
		FirstSeqNo:    op.SeqNo,
		FirstOffset:   op.FirstOffset,
		FirstChecksum: op.Checksum,
		LastSeqNo:     op.SeqNo,
		LastOffset:    op.LastOffset,
		Log:           op.Log,
	})
}
