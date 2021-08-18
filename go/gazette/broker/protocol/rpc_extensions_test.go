package protocol

import (
	"time"

	gc "github.com/go-check/check"
)

// RPCSuite tests RPC Request & Response validation cases by building instances
// broken in every conceivable way, and incrementally updating them until they
// pass validation.
type RPCSuite struct{}

func (s *RPCSuite) TestReadRequestValidation(c *gc.C) {
	var req = ReadRequest{
		Header:    badHeaderFixture(),
		Journal:   "/bad",
		Offset:    -2,
		EndOffset: -1,
	}
	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 12
	c.Check(req.Validate(), gc.ErrorMatches, `Journal: cannot begin with '/' \(/bad\)`)
	req.Journal = "good"
	c.Check(req.Validate(), gc.ErrorMatches, `invalid Offset \(-2; expected -1 <= Offset <= MaxInt64\)`)
	req.Offset = -1
	c.Check(req.Validate(), gc.ErrorMatches, `invalid EndOffset \(-1; expected 0 or Offset <= EndOffset\)`)
	req.EndOffset = 100

	c.Check(req.Validate(), gc.IsNil)

	req.Offset = 101
	c.Check(req.Validate(), gc.ErrorMatches, `invalid EndOffset \(100; expected 0 or Offset <= EndOffset\)`)

	req.EndOffset = 101
	c.Check(req.Validate(), gc.IsNil)
	req.EndOffset = 0
	c.Check(req.Validate(), gc.IsNil)

	// Block, DoNotProxy, and MetadataOnly have no validation.
}

func (s *RPCSuite) TestReadResponseValidationCases(c *gc.C) {
	var frag, _ = ParseFragmentFromRelativePath("a/journal",
		"00000000499602d2-000000008bd03835-0102030405060708090a0b0c0d0e0f1011121314.sz")
	frag.Journal = "/bad/name"

	var resp = ReadResponse{
		Status:      9101,
		Header:      badHeaderFixture(),
		Offset:      1234,
		WriteHead:   5678,
		Fragment:    &frag,
		FragmentUrl: ":/bad/url",
	}
	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status .*`)
	resp.Status = Status_OK

	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 12

	c.Check(resp.Validate(), gc.ErrorMatches, `Fragment.Journal: cannot begin with '/' \(/bad/name\)`)
	frag.Journal = "a/journal"

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid Offset \(1234; expected 1234567890 <= offset < \d+\)`)
	resp.Offset = 1234567891

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid WriteHead \(5678; expected >= 2345678901\)`)
	resp.WriteHead = 2345678901

	c.Check(resp.Validate(), gc.ErrorMatches, `FragmentUrl: parse ":/bad/url": missing protocol scheme`)
	resp.FragmentUrl = "http://foo"

	c.Check(resp.Validate(), gc.IsNil) // Success.

	// Remove Fragment.
	resp.Fragment = nil
	resp.WriteHead = -1

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid WriteHead \(-1; expected >= 0\)`)
	resp.WriteHead = 1234

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Offset without Fragment or Content \(\d+\)`)
	resp.Offset = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected FragmentUrl without Fragment \(http://foo\)`)
	resp.FragmentUrl = ""

	c.Check(resp.Validate(), gc.IsNil) // Success.

	// Set Content.
	resp.Content = []byte("foobar")
	resp.Fragment = &frag
	resp.Offset = 5678
	resp.FragmentUrl = "http://foo"
	resp.Status = Status_WRONG_ROUTE

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Status with Content \(WRONG_ROUTE\)`)
	resp.Status = Status_OK

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Header with Content \(process_id:.*`)
	resp.Header = nil

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected WriteHead with Content \(1234\)`)
	resp.WriteHead = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Fragment with Content \(journal:.*`)
	resp.Fragment = nil

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected FragmentUrl with Content \(http://foo\)`)
	resp.FragmentUrl = ""

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestAppendRequestValidationCases(c *gc.C) {
	var badLabel = LabelSet{Labels: []Label{{Name: "inv alid"}}}
	var goodLabel = LabelSet{Labels: []Label{{Name: "valid"}}}

	var req = AppendRequest{
		Header:            badHeaderFixture(),
		Journal:           "/bad",
		DoNotProxy:        true,
		Offset:            -1,
		Content:           []byte("foo"),
		CheckRegisters:    &LabelSelector{Include: badLabel},
		UnionRegisters:    &badLabel,
		SubtractRegisters: &badLabel,
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 12
	c.Check(req.Validate(), gc.ErrorMatches, `Journal: cannot begin with '/' \(/bad\)`)
	req.Journal = "good"
	c.Check(req.Validate(), gc.ErrorMatches, `invalid Offset \(-1; expected >= 0\)`)
	req.Offset = 100
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Content`)
	req.Content = nil
	c.Check(req.Validate(), gc.ErrorMatches, `CheckRegisters.Include.Labels\[0\].Name: not a valid token \(inv alid\)`)
	req.CheckRegisters.Include = goodLabel
	c.Check(req.Validate(), gc.ErrorMatches, `UnionRegisters.Labels\[0\].Name: not a valid token \(inv alid\)`)
	req.UnionRegisters = &goodLabel
	c.Check(req.Validate(), gc.ErrorMatches, `SubtractRegisters.Labels\[0\].Name: not a valid token \(inv alid\)`)
	req.SubtractRegisters = &goodLabel

	c.Check(req.Validate(), gc.IsNil)

	// Mark as a content-chunk request.
	req.Journal = ""
	req.Content = []byte("foo")

	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Header`)
	req.Header = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected DoNotProxy`)
	req.DoNotProxy = false
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Offset`)
	req.Offset = 0
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected CheckRegisters`)
	req.CheckRegisters = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected UnionRegisters`)
	req.UnionRegisters = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected SubtractRegisters`)
	req.SubtractRegisters = nil

	c.Check(req.Validate(), gc.IsNil)

	req = AppendRequest{} // Indicates Append should commit.
	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestAppendResponseValidationCases(c *gc.C) {
	var resp = AppendResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status .*`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 12
	c.Check(resp.Validate(), gc.ErrorMatches, `expected Commit`)
	resp.Commit = &Fragment{Journal: "/bad/name", CompressionCodec: CompressionCodec_NONE}
	c.Check(resp.Validate(), gc.ErrorMatches, `expected Registers`)
	resp.Registers = &LabelSet{Labels: []Label{{Name: "in valid"}}}
	c.Check(resp.Validate(), gc.ErrorMatches, `Commit.Journal: cannot begin with '/' \(/bad/name\)`)
	resp.Commit.Journal = "good/name"
	c.Check(resp.Validate(), gc.ErrorMatches, `Registers.Labels\[0\].Name: not a valid token \(in valid\)`)
	resp.Registers.Labels[0].Name = "valid"

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestReplicateRequestValidationCases(c *gc.C) {
	var req = ReplicateRequest{
		DeprecatedJournal: "bad",
		Proposal:          &Fragment{Journal: "/bad/name", CompressionCodec: CompressionCodec_NONE},
		Content:           []byte("foo"),
		ContentDelta:      100,
		Header:            badHeaderFixture(),
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 12
	c.Check(req.Validate(), gc.ErrorMatches, `expected Acknowledge with Header`)
	req.Acknowledge = true
	c.Check(req.Validate(), gc.ErrorMatches, `Proposal.Journal: cannot begin with '/' \(/bad/name\)`)
	req.Proposal.Journal = "journal"
	// TODO(johnny): Re-enable when Registers are required, post v0.83.
	// c.Check(req.Validate(), gc.ErrorMatches, `expected Registers with Proposal`)
	req.Registers = &LabelSet{Labels: []Label{{Name: "in valid"}}}
	c.Check(req.Validate(), gc.ErrorMatches, `Registers.Labels\[0\].Name: not a valid token \(in valid\)`)
	req.Registers.Labels[0].Name = "valid"
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Content with Proposal \(len 3\)`)
	req.Content = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected ContentDelta with Proposal \(100\)`)
	req.ContentDelta = 0
	c.Check(req.Validate(), gc.ErrorMatches, `DeprecatedJournal and Proposal.Journal mismatch \(bad vs journal\)`)
	req.DeprecatedJournal = "journal"

	c.Check(req.Validate(), gc.IsNil) // Success.

	// Clearing Proposal makes this a content chunk request.
	req.Proposal = nil
	req.Content = nil
	req.ContentDelta = -1

	c.Check(req.Validate(), gc.ErrorMatches, `expected Content or Proposal`)
	req.Content = []byte("foo")
	c.Check(req.Validate(), gc.ErrorMatches, `invalid ContentDelta \(-1; expected >= 0\)`)
	req.ContentDelta = 100
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Header with Content \(process_id:.*\)`)
	req.Header = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Registers with Content \(labels:.*\)`)
	req.Registers = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Acknowledge with Content`)
	req.Acknowledge = false
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected DeprecatedJournal with Content`)
	req.DeprecatedJournal = ""

	c.Check(req.Validate(), gc.IsNil) // Success.
}

func (s *RPCSuite) TestReplicateResponseValidationCases(c *gc.C) {
	var frag = &Fragment{Journal: "/bad/name", CompressionCodec: CompressionCodec_NONE}

	var resp = ReplicateResponse{
		Status:    9101,
		Header:    badHeaderFixture(),
		Fragment:  frag,
		Registers: new(LabelSet),
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status .*`)
	resp.Status = Status_OK

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Header \(process_id:.*\)`)
	resp.Header = nil
	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Fragment \(journal:.*\)`)
	resp.Fragment = nil
	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Registers \(\)`)
	resp.Registers = nil
	c.Check(resp.Validate(), gc.IsNil) // Success.

	resp.Status = Status_WRONG_ROUTE
	c.Check(resp.Validate(), gc.ErrorMatches, `expected Header`)
	resp.Header = badHeaderFixture()
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 12
	c.Check(resp.Validate(), gc.IsNil) // Success.

	resp.Status = Status_PROPOSAL_MISMATCH
	resp.Header = nil

	c.Check(resp.Validate(), gc.ErrorMatches, `expected Fragment`)
	resp.Fragment = frag
	c.Check(resp.Validate(), gc.ErrorMatches, `Fragment.Journal: cannot begin with '/' \(/bad/name\)`)
	frag.Journal = "journal"
	c.Check(resp.Validate(), gc.ErrorMatches, `expected Registers`)
	resp.Registers = &LabelSet{Labels: []Label{{Name: "in valid"}}}
	c.Check(resp.Validate(), gc.ErrorMatches, `Registers.Labels\[0\].Name: not a valid token \(in valid\)`)
	resp.Registers.Labels[0].Name = "valid"

	c.Check(resp.Validate(), gc.IsNil) // Success.
}

func (s *RPCSuite) TestListRequestValidationCases(c *gc.C) {
	var req = ListRequest{
		Selector: LabelSelector{
			Include: LabelSet{Labels: []Label{
				{Name: "a invalid name", Value: "foo"},
				{Name: "prefix", Value: "no/trailing/slash"},
			}},
			Exclude: LabelSet{Labels: []Label{
				{Name: "bar", Value: "baz"},
				{Name: "prefix", Value: "no/trailing/slash"},
			}},
		},
	}
	c.Check(req.Validate(), gc.ErrorMatches,
		`Selector.Include.Labels\[0\].Name: not a valid token \(a invalid name\)`)
	req.Selector.Include.Labels[0].Name = "a-valid-name"
	c.Check(req.Validate(), gc.ErrorMatches,
		`Selector.Include.Labels\["prefix"\]: expected trailing '/' \(no/trailing/slash\)`)
	req.Selector.Include.Labels[1].Value = "trailing/slash/"
	c.Check(req.Validate(), gc.ErrorMatches,
		`Selector.Exclude.Labels\["prefix"\]: expected trailing '/' \(no/trailing/slash\)`)
	req.Selector.Exclude.Labels[1].Value = "trailing/slash/"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestListResponseValidationCases(c *gc.C) {
	var resp = ListResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		Journals: []ListResponse_Journal{
			{
				ModRevision: 0,
				Spec: JournalSpec{
					Name:        "a/journal invalid name",
					Replication: 1,
					Fragment: JournalSpec_Fragment{
						Length:           1024,
						CompressionCodec: CompressionCodec_NONE,
						RefreshInterval:  time.Minute,
						Retention:        time.Hour,
					},
				},
				Route: Route{Primary: 0},
			},
		},
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `Journals\[0\].Spec.Name: not a valid token \(.*\)`)
	resp.Journals[0].Spec.Name = "a/journal"
	c.Check(resp.Validate(), gc.ErrorMatches, `Journals\[0\]: invalid ModRevision \(0; expected > 0\)`)
	resp.Journals[0].ModRevision = 1
	c.Check(resp.Validate(), gc.ErrorMatches, `Journals\[0\].Route: invalid Primary .*`)
	resp.Journals[0].Route.Primary = -1

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestApplyRequestValidationCases(c *gc.C) {
	var req = ApplyRequest{
		Changes: []ApplyRequest_Change{
			{
				ExpectModRevision: -2,
				Upsert:            &JournalSpec{Name: "a/journal"},
				Delete:            "a/journal",
			},
			{
				ExpectModRevision: 0,
				Delete:            "a/journal invalid name",
			},
			{
				ExpectModRevision: 1,
			},
		},
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: both Upsert and Delete are set \(expected exactly one\)`)
	req.Changes[0].Delete = ""

	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\].Upsert: invalid Replication .*`)
	req.Changes[0].Upsert = &JournalSpec{
		Name:        "a/journal",
		Replication: 1,
		Fragment: JournalSpec_Fragment{
			Length:           1024,
			CompressionCodec: CompressionCodec_NONE,
			RefreshInterval:  time.Minute,
			Retention:        time.Hour,
		},
	}
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: invalid ExpectModRevision \(-2; expected >= 0 or -1\)`)
	req.Changes[0].ExpectModRevision = 0
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\].Delete: not a valid token \(.*\)`)
	req.Changes[1].Delete = "a/journal"
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\]: invalid ExpectModRevision \(0; expected > 0 or -1\)`)
	req.Changes[1].ExpectModRevision = 1
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[2\]: neither Upsert nor Delete are set \(expected exactly one\)`)
	req.Changes[2].Delete = "other/journal"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestApplyResponseValidationCases(c *gc.C) {
	var resp = ApplyResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestFragmentsRequestValidationCases(c *gc.C) {
	var signatureTTL time.Duration

	var req = FragmentsRequest{
		Header:        badHeaderFixture(),
		Journal:       "/bad",
		SignatureTTL:  &signatureTTL,
		BeginModTime:  time.Unix(10, 0).Unix(),
		EndModTime:    time.Unix(5, 0).Unix(),
		NextPageToken: -1,
		PageLimit:     -1,
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 12
	c.Check(req.Validate(), gc.ErrorMatches, `Journal: cannot begin with '/' \(/bad\)`)
	req.Journal = "good"
	c.Check(req.Validate(), gc.ErrorMatches, `invalid EndModTime \(5 must be after 10\)`)
	req.EndModTime = 0
	c.Check(req.Validate(), gc.ErrorMatches, `invalid NextPageToken \(-1; must be >= 0\)`)
	req.NextPageToken = 10
	c.Check(req.Validate(), gc.ErrorMatches, `invalid PageLimit \(-1; must be >= 0 and <= 10000\)`)
	req.PageLimit = 10
	c.Check(req.Validate(), gc.ErrorMatches, `invalid SignatureTTL \(0s; must be > 0s\)`)
	signatureTTL = time.Second

	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestFragmentsResponseValidationCases(c *gc.C) {
	var resp = FragmentsResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		Fragments: []FragmentsResponse__Fragment{
			{
				Spec:      Fragment{Journal: "in valid", CompressionCodec: CompressionCodec_NONE},
				SignedUrl: ":gar: :bage:",
			},
		},
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `Fragments\[0\].Spec.Journal: not a valid token \(in valid\)`)
	resp.Fragments[0].Spec.Journal = "valid/name"
	c.Check(resp.Validate(), gc.ErrorMatches, `Fragments\[0\].SignedUrl: parse ":gar: :bage:": missing protocol scheme`)
	resp.Fragments[0].SignedUrl = "http://host/path"
	c.Check(resp.Validate(), gc.IsNil)
	resp.Fragments[0].SignedUrl = ""
	c.Check(resp.Validate(), gc.IsNil)
}

func badHeaderFixture() *Header {
	return &Header{
		ProcessId: ProcessSpec_ID{Zone: "zone", Suffix: "name"},
		Route:     Route{Primary: 0, Members: []ProcessSpec_ID{{Zone: "zone", Suffix: "name"}}},
		Etcd: Header_Etcd{
			ClusterId: 0, // ClusterId is invalid, but easily fixed up.
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

var _ = gc.Suite(&RPCSuite{})
