package recoverylog

import (
	"strconv"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
)

type FSMSuite struct {
	fsm *FSM
}

func (s *FSMSuite) SetUpTest(c *gc.C) {
	s.fsm = nil
}

func (s *FSMSuite) TestInitFromSeqNoZero(c *gc.C) {
	s.fsm = NewFSM(EmptyHints("a/log"))

	// Expect that operations starting from SeqNo 1 apply correctly.
	c.Check(s.create(1, 0x00000000, 100, "/path/A"), gc.IsNil)
	c.Check(s.link(2, 0x90f599e3, 100, 1, "/path/B"), gc.IsNil)

	c.Check(s.fsm.LogMark, gc.Equals, journal.NewMark("a/log", 2))
	c.Check(s.fsm.FirstSeqNo, gc.Equals, int64(1))
	c.Check(s.fsm.NextChecksum, gc.Equals, uint32(0x7355c460))
	c.Check(s.fsm.NextSeqNo, gc.Equals, int64(3))
}

func (s *FSMSuite) TestInitFromMidStream(c *gc.C) {
	s.fsm = NewFSM(EmptyHints("a/log"))

	// Expect that operations starting applying from SeqNo 42.
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.link(43, 0x2d28e063, 100, 42, "/path/B"), gc.IsNil)

	c.Check(s.fsm.LogMark, gc.Equals, journal.NewMark("a/log", 2))
	c.Check(s.fsm.FirstSeqNo, gc.Equals, int64(42))
	c.Check(s.fsm.NextChecksum, gc.Equals, uint32(0xf11e2261))
	c.Check(s.fsm.NextSeqNo, gc.Equals, int64(44))
}

func (s *FSMSuite) TestInitializationFromHints(c *gc.C) {
	s.fsm = NewFSM(FSMHints{
		LogMark:       journal.NewMark("a/log", 1234),
		FirstChecksum: 0xfeedbeef,
		FirstSeqNo:    42,
		Recorders:     []RecorderRange{{100, 50}, {200, 60}},
		SkipWrites:    map[Fnode]bool{45: true},
	})

	c.Check(s.fsm.LogMark, gc.Equals, journal.NewMark("a/log", 1234))
	c.Check(s.fsm.FirstSeqNo, gc.Equals, int64(42))
	c.Check(s.fsm.NextChecksum, gc.Equals, uint32(0xfeedbeef))
	c.Check(s.fsm.NextSeqNo, gc.Equals, int64(42))

	// Expect that operations only start applying from SeqNo 42.
	c.Check(s.create(41, 0xfeedbeef, 100, "/path/A"), gc.Equals, ErrWrongSeqNo)
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.link(43, 0xc132d1d7, 100, 42, "/path/B"), gc.IsNil)
}

func (s *FSMSuite) TestFnodeCreation(c *gc.C) {
	s.fsm = NewFSM(FSMHints{SkipWrites: map[Fnode]bool{44: true}})
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/another/path"), gc.IsNil)

	// Attempting to create an existing path fails.
	c.Check(s.create(44, 0xf11e2261, 100, "/path/A"), gc.Equals, ErrLinkExists)
	// Try again, with a valid path.
	c.Check(s.create(44, 0xf11e2261, 100, "/path/B"), gc.IsNil)

	// Expect Fnodes are tracked, and indexed on their links.
	c.Check(s.fsm.TrackedFnodes, gc.DeepEquals, []Fnode{42, 43, 44})
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/path/A":       42,
		"/another/path": 43,
		"/path/B":       44,
	})
	// Expect LiveNodes tracks creation checksum, offset, and links.
	// Also expect SkipWrites hint to have been applied.
	c.Check(s.fsm.LiveNodes, gc.DeepEquals, map[Fnode]FnodeState{
		42: FnodeState{
			Links:           map[string]struct{}{"/path/A": {}},
			CreatedChecksum: 0xfeedbeef,
			Offset:          1,
		},
		43: FnodeState{
			Links:           map[string]struct{}{"/another/path": {}},
			CreatedChecksum: 0x2d28e063,
			Offset:          2,
		},
		44: FnodeState{
			Links:           map[string]struct{}{"/path/B": {}},
			CreatedChecksum: 0xf11e2261,
			Offset:          4,
			SkipWrites:      true,
		},
	})
}

func (s *FSMSuite) TestFnodeLinking(c *gc.C) {
	s.fsm = NewFSM(EmptyHints("a/log"))

	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/another/path"), gc.IsNil)
	c.Check(s.link(44, 0xf11e2261, 100, 42, "/link/one"), gc.IsNil)

	// Attempting to link to an extant path errors.
	c.Check(s.link(45, 0xe292e757, 100, 43, "/path/A"), gc.Equals, ErrLinkExists)
	// ...even if the Fnode is less than FirstSeqNo.
	c.Check(s.link(45, 0xe292e757, 100, 15, "/path/A"), gc.Equals, ErrLinkExists)
	// As does attempting to link an unknown Fnode (44 is a link op, not a create).
	c.Check(s.link(45, 0xe292e757, 100, 44, "/link/two"), gc.Equals, ErrNoSuchFnode)

	// Link of an Fnode less than FirstSeqNo returns an ErrFnodeNotTracked.
	c.Check(s.link(45, 0xe292e757, 100, 15, "/link/two"), gc.Equals, ErrFnodeNotTracked)
	// Valid link. Expect that SeqNo/Checksum were incremented from ErrFnodeNotTracked.
	c.Check(s.link(46, 0x335952d4, 100, 43, "/link/two"), gc.IsNil)

	// Expect Links index and LiveNodes state reflects successful applies.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/path/A":       42,
		"/another/path": 43,
		"/link/one":     42,
		"/link/two":     43,
	})
	c.Check(s.fsm.LiveNodes, gc.DeepEquals, map[Fnode]FnodeState{
		42: FnodeState{
			Links:           map[string]struct{}{"/path/A": {}, "/link/one": {}},
			CreatedChecksum: 0xfeedbeef,
			Offset:          1,
		},
		43: FnodeState{
			Links:           map[string]struct{}{"/another/path": {}, "/link/two": {}},
			CreatedChecksum: 0x2d28e063,
			Offset:          2,
		},
	})
}

func (s *FSMSuite) TestFnodeUnlinking(c *gc.C) {
	s.fsm = NewFSM(EmptyHints("a/log"))

	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/another/path"), gc.IsNil)
	c.Check(s.link(44, 0xf11e2261, 200, 43, "/link/one"), gc.IsNil)

	// Precondition: 3 links exist.
	c.Check(s.fsm.Links, gc.HasLen, 3)

	// Attempting to unlink a non-existing path returns an error.
	c.Check(s.unlink(45, 0xe292e757, 200, 43, "/does/not/exist"), gc.Equals,
		ErrNoSuchLink)
	// As does attempting to unlink an existing path with an incorrect Fnode.
	c.Check(s.unlink(45, 0xe292e757, 200, 42, "/another/path"), gc.Equals,
		ErrNoSuchLink)

	// Unlink of an Fnode less than FirstSeqNo returns an ErrFnodeNotTracked.
	// However, SeqNo and Checksum are both incremented by this operation.
	c.Check(s.unlink(45, 0xe292e757, 200, 15, "/some/path"), gc.Equals,
		ErrFnodeNotTracked)

	// First unlink of 43. One remains. Both Fnodes are still live.
	c.Check(s.unlink(46, 0xc132d1d7, 200, 43, "/another/path"), gc.IsNil)

	// Expect /another/path was removed from Links & LiveNodes, but /link/one remains.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/path/A":   42,
		"/link/one": 43,
	})
	c.Check(s.fsm.LiveNodes[43].Links, gc.DeepEquals,
		map[string]struct{}{"/link/one": {}})
	c.Check(s.fsm.BuildHints().SkipWrites, gc.HasLen, 0) // No Fnodes are skippable.

	// Final unlink of 43. This destroys the Fnode.
	c.Check(s.unlink(47, 0xb18cc99a, 200, 43, "/link/one"), gc.IsNil)

	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{"/path/A": 42})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 1) // Only Fnode 42 remains.

	// Produced hints capture all three nodes; 43 is now skippable.
	c.Check(s.fsm.BuildHints(), gc.DeepEquals, FSMHints{
		LogMark:       journal.NewMark("a/log", 1),
		FirstChecksum: 0xfeedbeef,
		FirstSeqNo:    42,
		Recorders:     []RecorderRange{{100, 43}, {200, 47}},
		SkipWrites:    map[Fnode]bool{43: true},
	})

	// Expect that the Fnode & its recorder are still tracked (since 42 is live).
	c.Check(s.fsm.TrackedFnodes, gc.DeepEquals, []Fnode{42, 43})
	c.Check(s.fsm.Recorders, gc.DeepEquals, []RecorderRange{{100, 43}, {200, 47}})

	// Create Fnode 48, and then fully unlink Fnode 42.
	c.Check(s.create(48, 0x11bc1ac9, 300, "/other/path"), gc.IsNil)
	c.Check(s.unlink(49, 0xa102803e, 300, 42, "/path/A"), gc.IsNil)

	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{"/other/path": 48})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 1) // Just Fnode 48.

	// Expect that Fnodes 42 & 43 have been reclaimed, along with their recorders.
	c.Check(s.fsm.TrackedFnodes, gc.DeepEquals, []Fnode{48})
	c.Check(s.fsm.Recorders, gc.DeepEquals, []RecorderRange{{300, 49}})

	// Produced hints are now only sufficient to replay Fnode 48.
	c.Check(s.fsm.BuildHints(), gc.DeepEquals, FSMHints{
		LogMark:       journal.NewMark("a/log", 9),
		FirstChecksum: 0x11bc1ac9,
		FirstSeqNo:    48,
		Recorders:     []RecorderRange{{300, 49}},
		SkipWrites:    map[Fnode]bool{},
	})

	// Unlink 48 such that no live files remain.
	c.Check(s.unlink(50, 0xa40daee4, 400, 48, "/other/path"), gc.IsNil)

	// All tracking structures are now empty.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 0)
	c.Check(s.fsm.TrackedFnodes, gc.DeepEquals, []Fnode{})

	// Hints now reference the next expected op at the log head.
	c.Check(s.fsm.BuildHints(), gc.DeepEquals, FSMHints{
		LogMark:       journal.NewMark("a/log", 11),
		FirstChecksum: 0x5be3273b,
		FirstSeqNo:    51,
		Recorders:     []RecorderRange{},
		SkipWrites:    map[Fnode]bool{},
	})
}

func (s *FSMSuite) TestFnodeWrites(c *gc.C) {
	s.fsm = NewFSM(EmptyHints("a/log"))

	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)

	// Writes against known Fnodes succeed.
	c.Check(s.write(43, 0x2d28e063, 100, 42), gc.IsNil)
	c.Check(s.write(44, 0xf11e2261, 100, 42), gc.IsNil)

	// Writes against unknown Fnodes fail.
	c.Check(s.write(45, 0xe292e757, 100, 43), gc.Equals, ErrNoSuchFnode)
	// Writes against Fnodes < FirstSeqNo also fail, but increment SeqNo.
	c.Check(s.write(45, 0xe292e757, 100, 15), gc.Equals, ErrFnodeNotTracked)

	// Succeeds. Expect SeqNo was incremented from ErrFnodeNotTracked.
	c.Check(s.write(46, 0xd2622223, 100, 42), gc.IsNil)
}

func (s *FSMSuite) TestUseOfHintedRecorders(c *gc.C) {
	s.fsm = NewFSM(FSMHints{
		LogMark:   journal.Mark{Journal: "a/log"},
		Recorders: []RecorderRange{{100, 42}, {200, 44}, {300, 45}, {400, 47}}})

	// Intermix a "bad" recorder (666) which uses valid SeqNo & Checksums.
	// Expect that we still reconstruct the recorder-hinted history.
	c.Check(s.create(42, s.fsm.NextChecksum, 666, "/evil/path"), gc.Equals,
		ErrNotHinted)
	c.Check(s.create(42, s.fsm.NextChecksum, 100, "/good/path"), gc.IsNil)

	// A write from recorder 100 fails.
	c.Check(s.write(43, s.fsm.NextChecksum, 100, 42), gc.Equals, ErrNotHinted)
	// Two writes from recorder 200 for an untracked Fnode pass the recorder hint
	// check (though return an error). The third is unhinted, and fails.
	c.Check(s.write(43, s.fsm.NextChecksum, 200, 15), gc.Equals, ErrFnodeNotTracked)
	c.Check(s.write(44, s.fsm.NextChecksum, 200, 15), gc.Equals, ErrFnodeNotTracked)
	c.Check(s.write(45, s.fsm.NextChecksum, 200, 15), gc.Equals, ErrNotHinted)

	// Recorder 300 is valid for just one SeqNo.
	c.Check(s.write(45, s.fsm.NextChecksum, 666, 42), gc.Equals, ErrNotHinted)
	c.Check(s.write(45, s.fsm.NextChecksum, 300, 42), gc.IsNil)
	c.Check(s.write(46, s.fsm.NextChecksum, 300, 42), gc.Equals, ErrNotHinted)

	// Recorder 400 closes out the sequence.
	c.Check(s.create(46, s.fsm.NextChecksum, 400, "/another/path"), gc.IsNil)
	c.Check(s.write(47, s.fsm.NextChecksum, 400, 46), gc.IsNil)

	// Expect that produced hints are now identical to the input hints.
	c.Check(s.fsm.BuildHints().Recorders, gc.DeepEquals,
		[]RecorderRange{{100, 42}, {200, 44}, {300, 45}, {400, 47}})

	// Now that the hinted range has completed, any recorder is allowed.
	c.Check(s.unlink(48, s.fsm.NextChecksum, 666, 42, "/good/path"), gc.IsNil)
	c.Check(s.write(49, s.fsm.NextChecksum, 666, 46), gc.IsNil)

	c.Check(s.fsm.BuildHints().Recorders, gc.DeepEquals,
		[]RecorderRange{{400, 47}, {666, 49}})
}

func (s *FSMSuite) apply(op RecordedOp) error {
	// Ordinarily |op| bytes (as framed by the recorder) is digested by FSM to
	// produce updated checksums. To decouple these tests from the particular
	// protobuf encoding, here we maintain a total count of applied operations
	// per-test, and digest over its string conversion to produce unique but
	// stable 'frames' for each operation.
	s.fsm.LogMark.Offset += 1
	return s.fsm.Apply(&op, []byte(strconv.FormatInt(s.fsm.LogMark.Offset, 10)))
}

func (s *FSMSuite) create(seqNo int64, checksum uint32, rec uint32,
	path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Recorder: rec,
		Create: &RecordedOp_Create{Path: path}})
}

func (s *FSMSuite) link(seqNo int64, checksum uint32, rec uint32,
	fnode Fnode, path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Recorder: rec,
		Link: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *FSMSuite) unlink(seqNo int64, checksum uint32, rec uint32,
	fnode Fnode, path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Recorder: rec,
		Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *FSMSuite) write(seqNo int64, checksum uint32, rec uint32, fnode Fnode) error {
	// Write Length & Offset are ignored by FSM (though note they're captured
	// in the checksum digest FSM produces).
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Recorder: rec,
		Write: &RecordedOp_Write{Fnode: fnode}})
}

var _ = gc.Suite(&FSMSuite{})
