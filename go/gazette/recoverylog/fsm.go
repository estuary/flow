package recoverylog

import (
	"fmt"
	"hash/crc32"

	"github.com/pippio/gazette/journal"
)

var (
	ErrChecksumMismatch = fmt.Errorf("checksum mismatch")
	ErrFnodeNotTracked  = fmt.Errorf("fnode not tracked")
	ErrLinkExists       = fmt.Errorf("link exists")
	ErrNoSuchFnode      = fmt.Errorf("no such fnode")
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
// hints as to which Authors produced which SeqNo ranges in the final history.
type Author uint32

type FnodeState struct {
	// Active current paths of this Fnode.
	Links map[string]struct{}
	// True iff this Fnode has been hinted to be unlinked already at
	// a later point in the log, in which case writes may be ignored.
	SkipWrites bool
	// Checksum of the operation which created this Fnode.
	// Retained only to facilitate building FSMHints.
	CreatedChecksum uint32
	// Approximate offset of this Fnode's creation operation in the log.
	// Retained only to facilitate building FSMHints.
	Offset int64
}

// Processes which create RecordedOps are assigned a unique Author ID: a
// random, unique identifier which represents the process. It's used by FSM
// allow for reconcilliation of divergent histories in the recovery log, through
// hints as to which Authors produced which SeqNo ranges in the final history.
type AuthorRange struct {
	ID        Author
	LastSeqNo int64
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

	// First SeqNo encountered by this FSM.
	FirstSeqNo int64

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
	LiveNodes map[Fnode]FnodeState
	// Indexes current target paths of LiveNodes.
	Links map[string]Fnode

	// Fnodes which must be tracked for re-constructing the processing history of
	// this FSM. Namely, the first TrackedFnodes[0] is always live (with active
	// links). Successive Fnodes have greater SeqNo and may be live or dead.
	TrackedFnodes []Fnode
	// Encountered recorders of SeqNo ranges, ordered on RecoderRange.LastSeqNo.
	// AuthorRanges are reclaimed when they no longer cover a TrackedFnode.
	Authors []AuthorRange

	// Hints are used for a few purposes:
	// * Determining the journal Mark, SeqNo, and checksum which playback
	//   should commence from.
	// * Identifying encountered Fnodes which will be fully unlinked prior to log
	//   end, and for which local writes may be skipped during playback.
	// * Consistently resolving any branches in the log. Specifically, a branch
	//   is reflected by two operations with identical SeqNo and Checksum from
	//   two different recorders. If a hinted range is available FSM will prefer
	//   the hinted recorder, enabling exact correspondence to the hinted history.
	hints FSMHints
}

// Memoized state which allows an FSM to most-efficiently reach parity with
// the FSM which produced the FSMHints.
type FSMHints struct {
	// (Potentially approximate) lower-bound log mark to begin reading from.
	LogMark journal.Mark

	// First Checksum & SeqNo to begin processing from.
	FirstChecksum uint32
	FirstSeqNo    int64

	// Captures the recoders of processed operations, starting from FirstSeqNo.
	Authors []AuthorRange `json:"Recorders"`
	// Ordered Fnodes which are unlinked between FirstSeqNo and the log head.
	SkipWrites []Fnode

	// Property files and contents. See FSM.Properties.
	Properties map[string]string
}

func EmptyHints(log journal.Name) FSMHints {
	return FSMHints{LogMark: journal.Mark{Journal: log}}
}

func NewFSM(hints FSMHints) *FSM {
	return &FSM{
		LogMark:       hints.LogMark,
		NextSeqNo:     hints.FirstSeqNo,
		NextChecksum:  hints.FirstChecksum,
		FirstSeqNo:    hints.FirstSeqNo,
		Properties:    hints.Properties,
		LiveNodes:     make(map[Fnode]FnodeState),
		Links:         make(map[string]Fnode),
		TrackedFnodes: []Fnode{},
		Authors:       []AuthorRange{},
		hints:         hints,
	}
}

func (m *FSM) Apply(op *RecordedOp, frame []byte) error {
	if m.NextSeqNo == 0 {
		// Initial condition: assign NextSeqNo and Checksum from first SeqNo seen.
		m.FirstSeqNo = op.SeqNo
		m.NextChecksum = op.Checksum
		m.NextSeqNo = op.SeqNo
	}

	if op.SeqNo == 0 || op.SeqNo != m.NextSeqNo {
		return ErrWrongSeqNo
	} else if op.Checksum != m.NextChecksum {
		return ErrChecksumMismatch
	}

	// If hints remain, ensure that op.Author is hinted for this op.SeqNo.
	if len(m.hints.Authors) != 0 && m.hints.Authors[0].ID != op.Author {
		// This is a consistent operation, but written by a non-hinted Author
		// for this SeqNo: the operation represents a (likely dead) branch in
		// recovery-log history relative to the FSMHints we're re-building.
		return ErrNotHinted
	}

	var err error
	if op.Create != nil {
		err = m.applyCreate(op)
	} else if op.Link != nil {
		err = m.applyLink(op.Link)
	} else if op.Unlink != nil {
		err = m.applyUnlink(op.Unlink)
	} else if op.Write != nil {
		err = m.applyWrite(op.Write)
	} else if op.Property != nil {
		err = m.applyProperty(op.Property)
	}

	if err == nil || err == ErrFnodeNotTracked {
		m.NextSeqNo += 1
		m.NextChecksum = crc32.Update(m.NextChecksum, crcTable, frame)

		// Capture a change in Author.
		if l := len(m.Authors); l == 0 || m.Authors[l-1].ID != op.Author {
			m.Authors = append(m.Authors, AuthorRange{ID: op.Author})
		}
		m.Authors[len(m.Authors)-1].LastSeqNo = op.SeqNo

		// Pop a hinted recorder whose SeqNo range has just completed.
		if len(m.hints.Authors) != 0 && m.hints.Authors[0].LastSeqNo == op.SeqNo {
			m.hints.Authors = m.hints.Authors[1:]
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

	// Determine whether there's a hint to skip writes for |fnode|.
	var skipWrites bool
	if len(m.hints.SkipWrites) != 0 && m.hints.SkipWrites[0] == fnode {
		m.hints.SkipWrites = m.hints.SkipWrites[1:] // Remove the hint.
		skipWrites = true
	}

	m.LiveNodes[fnode] = FnodeState{
		CreatedChecksum: op.Checksum,
		Offset:          m.LogMark.Offset,
		Links:           map[string]struct{}{op.Create.Path: {}},
		SkipWrites:      skipWrites,
	}

	m.Links[op.Create.Path] = fnode
	m.TrackedFnodes = append(m.TrackedFnodes, fnode)

	return nil
}

func (m *FSM) applyLink(op *RecordedOp_Link) error {
	if _, ok := m.Links[op.Path]; ok {
		return ErrLinkExists
	} else if _, ok := m.Properties[op.Path]; ok {
		return ErrPropertyExists
	} else if int64(op.Fnode) < m.FirstSeqNo {
		return ErrFnodeNotTracked
	}
	node, ok := m.LiveNodes[op.Fnode]
	if !ok {
		return ErrNoSuchFnode
	}

	node.Links[op.Path] = struct{}{}
	m.Links[op.Path] = op.Fnode
	return nil
}

func (m *FSM) applyUnlink(op *RecordedOp_Link) error {
	if int64(op.Fnode) < m.FirstSeqNo {
		return ErrFnodeNotTracked
	}
	node, ok := m.LiveNodes[op.Fnode]
	if !ok {
		return ErrNoSuchFnode
	}

	if _, ok := node.Links[op.Path]; !ok {
		return ErrNoSuchLink
	}
	delete(m.Links, op.Path)
	delete(node.Links, op.Path)

	if len(node.Links) != 0 {
		// Fnode has remaining live links.
		return nil
	}
	// Fnode is no longer live (all links are removed).
	delete(m.LiveNodes, op.Fnode)

	// Walk through TrackedFnodes, reclaiming a contiguous prefix of dead entries.
	for len(m.TrackedFnodes) != 0 {
		if _, live := m.LiveNodes[m.TrackedFnodes[0]]; live {
			break
		}
		m.TrackedFnodes = m.TrackedFnodes[1:]
	}
	// Reclaim Authors with a LastSeqNo occurring before all TrackedFnodes.
	for len(m.Authors) != 0 {
		if len(m.TrackedFnodes) != 0 &&
			m.Authors[0].LastSeqNo >= int64(m.TrackedFnodes[0]) {
			break
		}
		m.Authors = m.Authors[1:]
	}
	return nil
}

func (m *FSM) applyWrite(op *RecordedOp_Write) error {
	if int64(op.Fnode) < m.FirstSeqNo {
		return ErrFnodeNotTracked
	}
	_, ok := m.LiveNodes[op.Fnode]
	if !ok {
		return ErrNoSuchFnode
	}
	return nil
}

func (m *FSM) applyProperty(op *RecordedOp_Property) error {
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

// Constructs memoized hints enabling a future FSM to rebuild this FSM's state.
func (m *FSM) BuildHints() FSMHints {
	// Init hints to the next expected op in the log.
	hints := FSMHints{
		LogMark:       m.LogMark,
		FirstChecksum: m.NextChecksum,
		FirstSeqNo:    m.NextSeqNo,
		Authors:       []AuthorRange{},
		Properties:    m.Properties,
	}
	// Retrieve the earliest SeqNo, Checksum, and Offset of a live Fnode. Note
	// an invariant of FSM is that the first |TrackedFnodes| is in |LiveNodes|.
	for _, fnode := range m.TrackedFnodes {
		if state, isLive := m.LiveNodes[fnode]; isLive {
			if int64(fnode) < hints.FirstSeqNo {
				hints.FirstSeqNo = int64(fnode)
				hints.FirstChecksum = state.CreatedChecksum
				hints.LogMark.Offset = state.Offset
			}
		} else {
			hints.SkipWrites = append(hints.SkipWrites, fnode)
		}
	}
	// Only return recorder ranges if tracked Fnodes are hinted.
	if len(m.TrackedFnodes) != 0 {
		hints.Authors = append([]AuthorRange{}, m.Authors...)
	}
	return hints
}

func (m *FSM) HasHints() bool {
	return len(m.hints.Authors) != 0 || len(m.hints.SkipWrites) != 0
}
