package recoverylog

import (
	"strconv"

	gc "github.com/go-check/check"
)

type FSMSuite struct {
	fsm *FSM

	// Ordinarily the framed operation bytes are digested by FSM to produce
	// updated checksums. To decouple these tests from the particular encoding,
	// we digest over the string value of an offset maintained by the test to
	// produce unique but stable 'frames' for each operation. In other words,
	// |offset| determines the checksum fixtures used in these tests.
	offset int64
}

func (s *FSMSuite) SetUpTest(c *gc.C) {
	s.fsm = nil
	s.offset = 0
}

func (s *FSMSuite) TestInitFromSeqNoZero(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{Log: aRecoveryLog})

	// Expect that operations starting from SeqNo 1 apply correctly.
	c.Check(s.create(1, 0x00000000, 100, "/path/A"), gc.IsNil)
	c.Check(s.link(2, 0x90f599e3, 100, 1, "/path/B"), gc.IsNil)

	c.Check(s.fsm.NextChecksum, gc.Equals, uint32(0x7355c460))
	c.Check(s.fsm.NextSeqNo, gc.Equals, int64(3))
	c.Check(s.fsm.LastLog, gc.Equals, aRecoveryLog)
}

func (s *FSMSuite) TestFlatteningLiveLogSegments(c *gc.C) {
	var hints = FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 2, Segments: []Segment{
				{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 700, FirstChecksum: 0x22},
			}},
			{Fnode: 4, Segments: []Segment{
				{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901, FirstChecksum: 0x44},
			}},
			{Fnode: 10, Segments: []Segment{
				{Author: 0x2, FirstSeqNo: 10, LastSeqNo: 10, FirstOffset: 1000, LastOffset: 1001, FirstChecksum: 0x10, Log: "other"},
			}},
		},
	}

	var fnodes, set, err = hints.LiveLogSegments()
	c.Check(err, gc.IsNil)
	c.Check(fnodes, gc.DeepEquals, []Fnode{2, 4, 10})
	c.Check(set, gc.DeepEquals, SegmentSet{
		{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 9, FirstOffset: 200, LastOffset: 901, FirstChecksum: 0x22, Log: aRecoveryLog},
		{Author: 0x2, FirstSeqNo: 10, LastSeqNo: 10, FirstOffset: 1000, LastOffset: 1001, FirstChecksum: 0x10, Log: "other"},
	})

	// Expect it complains if Fnode != FirstSeqNo.
	hints.LiveNodes = append(hints.LiveNodes, FnodeSegments{Fnode: 13, Segments: []Segment{{FirstSeqNo: 14}}})
	_, _, err = hints.LiveLogSegments()
	c.Check(err, gc.ErrorMatches, "expected Fnode to match Segment FirstSeqNo: .*")

	// Or if segments are inconsistent.
	hints.LiveNodes[1].Segments[0].Author = 0x3
	_, _, err = hints.LiveLogSegments()
	c.Check(err, gc.ErrorMatches, "expected Segment Author equality: .*")

	// Or if Fnodes are mis-ordered.
	hints.LiveNodes[1], hints.LiveNodes[0] = hints.LiveNodes[0], hints.LiveNodes[1]
	_, _, err = hints.LiveLogSegments()
	c.Check(err, gc.ErrorMatches, "expected monotonic Fnode ordering: .*")
}

func (s *FSMSuite) TestInitializationFromHints(c *gc.C) {
	var hints = FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				{
					Author:        100,
					FirstChecksum: 0xfeedbeef,
					FirstOffset:   1234, // Lower-bound. Actual offset is 1236.
					FirstSeqNo:    42,
					LastSeqNo:     43,
				}}},
		},
		Properties: []Property{{Path: "/IDENTITY", Content: "foo-bar-baz"}},
	}
	s.fsm = s.newFSM(c, hints)

	c.Check(s.fsm.hintedSegments[0].FirstOffset, gc.Equals, int64(1234))

	c.Check(s.fsm.NextChecksum, gc.Equals, uint32(0xfeedbeef))
	c.Check(s.fsm.NextSeqNo, gc.Equals, int64(42))
	c.Check(s.fsm.LastLog, gc.Equals, aRecoveryLog)
	c.Check(s.fsm.Properties, gc.DeepEquals,
		map[string]string{"/IDENTITY": "foo-bar-baz"})

	s.offset = 1234 // Skip "offset" forward to first hinted value.

	// Expect that operations only start applying from SeqNo 42.
	c.Check(s.create(41, 0xfeedbeef, 100, "/path/A"), gc.Equals, ErrWrongSeqNo)
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.link(43, 0xc132d1d7, 100, 42, "/path/B"), gc.IsNil)

	// Expect hints reflect operations 42 & 43, and pass-through Properties.
	// Tweak |hints| to reflect that FSM observed "true" offsets for FirstOffset/LastOffset.
	hints.LiveNodes[0].Segments[0].FirstOffset = 1236
	hints.LiveNodes[0].Segments[0].LastOffset = 1238

	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, hints)
}

func (s *FSMSuite) TestFnodeCreation(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{
		Log:        aRecoveryLog,
		Properties: []Property{{Path: "/property/path", Content: "content"}},
	})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/another/path"), gc.IsNil)

	// Attempting to create an existing path fails.
	c.Check(s.create(44, 0xf11e2261, 100, "/path/A"), gc.Equals, ErrLinkExists)
	// As does attempting to create an existing property.
	c.Check(s.create(44, 0xf11e2261, 100, "/property/path"), gc.Equals, ErrPropertyExists)
	// Try again, with a valid path.
	c.Check(s.create(44, 0xf11e2261, 100, "/path/B"), gc.IsNil)

	// Expect Fnodes are tracked, and indexed on their links.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/path/A":       42,
		"/another/path": 43,
		"/path/B":       44,
	})
	// Expect LiveNodes tracks Segments and links.
	c.Check(s.fsm.LiveNodes, gc.DeepEquals, map[Fnode]*fnodeState{
		42: {
			Links: map[string]struct{}{"/path/A": {}},
			Segments: []Segment{{Author: 100, FirstSeqNo: 42, FirstOffset: 1,
				FirstChecksum: 0xfeedbeef, LastSeqNo: 42, LastOffset: 2, Log: aRecoveryLog}},
		},
		43: {
			Links: map[string]struct{}{"/another/path": {}},
			Segments: []Segment{{Author: 100, FirstSeqNo: 43, FirstOffset: 2,
				FirstChecksum: 0x2d28e063, LastSeqNo: 43, LastOffset: 3, Log: aRecoveryLog}},
		},
		44: {
			Links: map[string]struct{}{"/path/B": {}},
			Segments: []Segment{{Author: 100, FirstSeqNo: 44, FirstOffset: 5,
				FirstChecksum: 0xf11e2261, LastSeqNo: 44, LastOffset: 6, Log: aRecoveryLog}},
		},
	})
}

func (s *FSMSuite) TestFnodeCreationNotHinted(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{{Author: 100, FirstChecksum: 0xfeedbeef,
				FirstOffset: 1234, FirstSeqNo: 42, LastSeqNo: 100}}},
			{Fnode: 44, Segments: []Segment{{Author: 100, FirstChecksum: 0xf11e2261,
				FirstOffset: 1234, FirstSeqNo: 44, LastSeqNo: 100}}},
		},
	})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	// Expect the first and third Fnodes are created, while 43 is not tracked.
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/another/path"), gc.Equals,
		ErrFnodeNotTracked)
	c.Check(s.create(44, 0xf11e2261, 100, "/final/path"), gc.IsNil)

	// Expect Links and LiveNodes tracks only Fnodes 42 & 44.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/path/A":     42,
		"/final/path": 44,
	})
	c.Check(s.fsm.LiveNodes, gc.DeepEquals, map[Fnode]*fnodeState{
		42: {
			Links: map[string]struct{}{"/path/A": {}},
			Segments: []Segment{{Author: 100, FirstSeqNo: 42, FirstOffset: 1,
				FirstChecksum: 0xfeedbeef, LastSeqNo: 42, LastOffset: 2, Log: aRecoveryLog}},
		},
		44: {
			Links: map[string]struct{}{"/final/path": {}},
			Segments: []Segment{{Author: 100, FirstSeqNo: 44, FirstOffset: 3,
				FirstChecksum: 0xf11e2261, LastSeqNo: 44, LastOffset: 4, Log: aRecoveryLog}},
		},
	})
}

func (s *FSMSuite) TestHintedFnodeNotCreated(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{{Author: 100, FirstChecksum: 0xfeedbeef,
				FirstOffset: 1234, FirstSeqNo: 42, LastSeqNo: 100}}},
			{Fnode: 44, Segments: []Segment{{Author: 100, FirstChecksum: 0xf11e2261,
				FirstOffset: 1234, FirstSeqNo: 44, LastSeqNo: 100}}},
		},
	})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	// Expect op 44 produces an error, despite being properly sequenced, because an FNode is expected.
	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)
	c.Check(s.write(43, s.fsm.NextChecksum, 100, 42), gc.IsNil)
	c.Check(s.write(44, s.fsm.NextChecksum, 100, 42), gc.Equals, ErrExpectedHintedFnode)
}

func (s *FSMSuite) TestFnodeLinking(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{Log: "empty/log"})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	c.Check(s.create(42, 0xfeedbeef, 100, "/existing/path"), gc.IsNil)
	c.Check(s.create(43, 0x2d28e063, 100, "/source/path"), gc.IsNil)
	c.Check(s.link(44, 0xf11e2261, 100, 42, "/target/one"), gc.IsNil)
	c.Check(s.property(45, 0xe292e757, 100, "/property/path", "content"), gc.IsNil)

	// Attempting to link to an extant path errors.
	c.Check(s.link(46, 0x2009a120, 100, 43, "/existing/path"), gc.Equals, ErrLinkExists)
	// ...even if the Fnode is less than FirstSeqNo.
	c.Check(s.link(46, 0x2009a120, 100, 15, "/existing/path"), gc.Equals, ErrLinkExists)
	// As does linking to an existing property.
	c.Check(s.link(46, 0x2009a120, 100, 44, "/property/path"), gc.Equals, ErrPropertyExists)

	// Link of an unknown Fnode returns ErrFnodeNotTracked.
	c.Check(s.link(46, 0x2009a120, 100, 15, "/target/two"), gc.Equals, ErrFnodeNotTracked)
	// Valid link under a new author. Expect that SeqNo/Checksum were incremented
	// from ErrFnodeNotTracked.
	c.Check(s.link(47, 0xc8dac550, 200, 43, "/target/two"), gc.IsNil)

	// Expect Links index and LiveNodes state reflects successful applies.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{
		"/existing/path": 42,
		"/source/path":   43,
		"/target/one":    42,
		"/target/two":    43,
	})
	c.Check(s.fsm.LiveNodes, gc.DeepEquals, map[Fnode]*fnodeState{
		42: {
			Links: map[string]struct{}{"/existing/path": {}, "/target/one": {}},
			Segments: []Segment{
				// Expect Link operation extended current author segment.
				{Author: 100, FirstSeqNo: 42, FirstOffset: 1,
					FirstChecksum: 0xfeedbeef, LastSeqNo: 44, LastOffset: 4, Log: aRecoveryLog}},
		},
		43: {
			Links: map[string]struct{}{"/source/path": {}, "/target/two": {}},
			Segments: []Segment{
				// Under a different author, a new Segment was appended.
				{Author: 100, FirstSeqNo: 43, FirstOffset: 2,
					FirstChecksum: 0x2d28e063, LastSeqNo: 43, LastOffset: 3, Log: aRecoveryLog},
				{Author: 200, FirstSeqNo: 47, FirstOffset: 9,
					FirstChecksum: 0xc8dac550, LastSeqNo: 47, LastOffset: 10, Log: aRecoveryLog},
			},
		},
	})
	c.Check(s.fsm.Properties, gc.DeepEquals, map[string]string{
		"/property/path": "content"})
}

func (s *FSMSuite) TestFnodeUnlinking(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{Log: "empty/log"})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

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

	// Unlink of an unknown Fnode returns an ErrFnodeNotTracked.
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
	c.Check(s.fsm.LiveNodes, gc.HasLen, 2) // Both Fnodes are still live.

	// Hints reflect both Fnode 42 & 43.
	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				{Author: 0x64, FirstSeqNo: 42, FirstOffset: 1,
					FirstChecksum: 0xfeedbeef, LastSeqNo: 42, LastOffset: 2}}},
			{Fnode: 43, Segments: []Segment{
				{Author: 0x64, FirstSeqNo: 43, FirstOffset: 2,
					FirstChecksum: 0x2d28e063, LastSeqNo: 43, LastOffset: 3},
				// Expect unlink operation extended the Fnode's Segments.
				{Author: 0xc8, FirstSeqNo: 44, FirstOffset: 3,
					FirstChecksum: 0xf11e2261, LastSeqNo: 46, LastOffset: 8}}},
		},
	})

	// Final unlink of 43. This destroys the Fnode.
	c.Check(s.unlink(47, 0xb18cc99a, 200, 43, "/link/one"), gc.IsNil)

	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{"/path/A": 42})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 1) // Only Fnode 42 remains.

	// Produced hints capture Fnode 42 only.
	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				{Author: 0x64, FirstSeqNo: 42, FirstOffset: 1,
					FirstChecksum: 0xfeedbeef, LastSeqNo: 42, LastOffset: 2}}},
		},
	})

	// Create Fnode 48, and then fully unlink Fnode 42.
	c.Check(s.create(48, 0x11bc1ac9, 300, "/other/path"), gc.IsNil)
	c.Check(s.unlink(49, 0xa102803e, 300, 42, "/path/A"), gc.IsNil)

	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{"/other/path": 48})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 1) // Just Fnode 48.

	// Produced hints are now only sufficient to replay Fnode 48.
	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 48, Segments: []Segment{
				{Author: 0x12c, FirstSeqNo: 48, FirstOffset: 9,
					FirstChecksum: 0x11bc1ac9, LastSeqNo: 48, LastOffset: 10}}},
		},
	})

	// Unlink 48 such that no live files remain.
	c.Check(s.unlink(50, 0xa40daee4, 400, 48, "/other/path"), gc.IsNil)

	// All tracking structures are now empty.
	c.Check(s.fsm.Links, gc.DeepEquals, map[string]Fnode{})
	c.Check(s.fsm.LiveNodes, gc.HasLen, 0)

	// Hints are now empty.
	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{Log: aRecoveryLog})
}

func (s *FSMSuite) TestPropertyUpdates(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{
		Log:        aRecoveryLog,
		Properties: []Property{{Path: "/a/property", Content: "content"}},
	})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	c.Check(s.create(42, 0xfeedbeef, 100, "/a/path"), gc.IsNil)

	// Create a new property. Expect it's recorded in Properties.
	c.Check(s.property(43, 0x2d28e063, 100, "/another/property", "other-content"), gc.IsNil)
	c.Check(s.fsm.Properties, gc.DeepEquals, map[string]string{
		"/a/property":       "content",
		"/another/property": "other-content",
	})

	// Attempting a property update of an existing file fails.
	c.Check(s.property(44, 0xf11e2261, 100, "/a/path", "bad"), gc.Equals, ErrLinkExists)

	// Attempting a property update of an existing property fails. We may change
	// this in the future, if a sufficient motivating case appears, but for now
	// we apply the most restrictive behavior.
	c.Check(s.property(44, 0xf11e2261, 100, "/a/property", "update"), gc.Equals,
		ErrPropertyExists)

	c.Check(s.fsm.Properties, gc.DeepEquals, map[string]string{
		"/a/property":       "content",
		"/another/property": "other-content",
	})
}

func (s *FSMSuite) TestFnodeWrites(c *gc.C) {
	s.fsm = s.newFSM(c, FSMHints{Log: aRecoveryLog})
	s.fsm.NextSeqNo, s.fsm.NextChecksum = 42, 0xfeedbeef

	c.Check(s.create(42, 0xfeedbeef, 100, "/path/A"), gc.IsNil)

	// Writes against known Fnodes succeed.
	c.Check(s.write(43, 0x2d28e063, 100, 42), gc.IsNil)
	c.Check(s.write(44, 0xf11e2261, 100, 42), gc.IsNil)

	// Writes against unknown Fnodes fail.
	c.Check(s.write(45, 0xe292e757, 100, 15), gc.Equals, ErrFnodeNotTracked)

	// Succeeds. Expect SeqNo was incremented from ErrFnodeNotTracked.
	c.Check(s.write(46, 0x2009a120, 100, 42), gc.IsNil)

	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				// Expect write operations extended Fnode Segments.
				{Author: 0x64, FirstSeqNo: 42, FirstOffset: 1,
					FirstChecksum: 0xfeedbeef, LastSeqNo: 46, LastOffset: 6}}},
		},
	})
}

func (s *FSMSuite) TestUseOfHintedAuthors(c *gc.C) {
	var hints = FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				{Author: 100, FirstOffset: 2, FirstSeqNo: 42, LastSeqNo: 42, LastOffset: 3},
				{Author: 200, FirstOffset: 5, FirstSeqNo: 44, LastSeqNo: 44, LastOffset: 6},
				{Author: 300, FirstOffset: 8, FirstSeqNo: 45, LastSeqNo: 45, LastOffset: 9},
			}},
			{Fnode: 46, Segments: []Segment{
				{Author: 400, FirstOffset: 11, FirstSeqNo: 46, LastSeqNo: 47, LastOffset: 13}}},
		},
	}
	s.fsm = s.newFSM(c, hints)

	// Intermix a "bad" recorder (666) which uses valid SeqNo & Checksums.
	// Expect that we still reconstruct the recorder-hinted history.
	c.Check(s.create(42, 0x0, 666, "/evil/path"), gc.Equals, ErrNotHintedAuthor)
	c.Check(s.create(42, 0x0, 100, "/good/path"), gc.IsNil)

	// A write from recorder 100 fails (200 is hinted next).
	c.Check(s.write(43, s.fsm.NextChecksum, 100, 42), gc.Equals, ErrNotHintedAuthor)
	// Operation 43 is not hinted by a Segment. Recorder 200 is hinted for 44.
	c.Check(s.write(43, s.fsm.NextChecksum, 200, 15), gc.Equals, ErrWrongSeqNo)
	c.Check(s.write(44, 0x0, 200, 42), gc.IsNil) // Expect checksum was reset.
	c.Check(s.write(45, s.fsm.NextChecksum, 200, 42), gc.Equals, ErrNotHintedAuthor)

	// Author 300 is valid for just one SeqNo (45).
	c.Check(s.write(45, s.fsm.NextChecksum, 666, 42), gc.Equals, ErrNotHintedAuthor)
	c.Check(s.write(45, 0x0, 300, 42), gc.IsNil) // Expect checksum was reset.
	c.Check(s.write(46, s.fsm.NextChecksum, 300, 42), gc.Equals, ErrNotHintedAuthor)

	// Author 400 closes out the sequence.
	c.Check(s.write(46, s.fsm.NextChecksum, 666, 42), gc.Equals, ErrNotHintedAuthor)
	c.Check(s.create(46, 0x0, 400, "/another/path"), gc.IsNil)
	c.Check(s.write(47, s.fsm.NextChecksum, 400, 46), gc.IsNil)

	// Expect that produced hints are now identical to the input hints.
	c.Check(s.fsm.BuildHints(aRecoveryLog).LiveNodes, gc.DeepEquals, hints.LiveNodes)

	// Now that the hinted range has completed, any recorder is allowed.
	c.Check(s.unlink(48, s.fsm.NextChecksum, 666, 42, "/good/path"), gc.IsNil)
	c.Check(s.write(49, s.fsm.NextChecksum, 666, 46), gc.IsNil)

	c.Check(s.fsm.BuildHints(aRecoveryLog), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{{Fnode: 46, Segments: []Segment{
			{Author: 400, FirstSeqNo: 46, FirstOffset: 11, LastSeqNo: 47, LastOffset: 13},
			{Author: 666, FirstSeqNo: 49, FirstOffset: 14,
				FirstChecksum: 0x1cab6124, LastSeqNo: 49, LastOffset: 15},
		}}},
	})
}

func (s *FSMSuite) apply(op RecordedOp) error {
	// Create a unique "frame" from |offset| for FSM to digest over, in production of checksums.
	s.offset++
	var frame = []byte(strconv.FormatInt(s.offset, 10))

	op.FirstOffset = s.offset
	op.LastOffset = s.offset + 1
	op.Log = aRecoveryLog

	return s.fsm.Apply(&op, frame)
}

func (s *FSMSuite) create(seqNo int64, checksum uint32, auth Author, path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Author: auth,
		Create: &RecordedOp_Create{Path: path}})
}

func (s *FSMSuite) link(seqNo int64, checksum uint32, auth Author,
	fnode Fnode, path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Author: auth,
		Link: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *FSMSuite) unlink(seqNo int64, checksum uint32, auth Author,
	fnode Fnode, path string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Author: auth,
		Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *FSMSuite) write(seqNo int64, checksum uint32, auth Author, fnode Fnode) error {
	// Write Length & Offset are ignored by FSM (though note they're captured
	// in the checksum digest FSM produces).
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Author: auth,
		Write: &RecordedOp_Write{Fnode: fnode}})
}

func (s *FSMSuite) property(seqNo int64, checksum uint32, auth Author,
	path, content string) error {
	return s.apply(RecordedOp{SeqNo: seqNo, Checksum: checksum, Author: auth,
		Property: &Property{Path: path, Content: content}})
}

func (s *FSMSuite) newFSM(c *gc.C, hints FSMHints) *FSM {
	fsm, err := NewFSM(hints)
	c.Assert(err, gc.IsNil)
	return fsm
}

var _ = gc.Suite(&FSMSuite{})
