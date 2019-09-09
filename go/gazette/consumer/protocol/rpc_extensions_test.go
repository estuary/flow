package protocol

import (
	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
)

type RPCSuite struct{}

func (s *RPCSuite) TestStatRequestValidationCases(c *gc.C) {
	var req = StatRequest{
		Header: badHeaderFixture(),
		Shard:  "invalid shard",
	}
	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 1234
	c.Check(req.Validate(), gc.ErrorMatches, `Shard: not a valid token \(invalid shard\)`)
	req.Shard = "valid-shard"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestStatResponseValidationCases(c *gc.C) {
	var resp = StatResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		ReadThrough: pb.Offsets{
			"a/journal": -123,
		},
		PublishAt: pb.Offsets{
			"other/journal": -456,
		},
	}
	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `ReadThrough.Offsets\[a/journal\]: invalid offset \(-123; expected >= 0\)`)
	resp.ReadThrough["a/journal"] = 123
	c.Check(resp.Validate(), gc.ErrorMatches, `PublishAt.Offsets\[other/journal\]: invalid offset \(-456; expected >= 0\)`)
	resp.PublishAt["other/journal"] = 456

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestListRequestValidationCases(c *gc.C) {
	var req = ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "a invalid name", Value: "foo"}}},
			Exclude: pb.MustLabelSet("id", "bar"),
		},
	}
	c.Check(req.Validate(), gc.ErrorMatches,
		`Selector.Include.Labels\[0\].Name: not a valid token \(a invalid name\)`)
	req.Selector.Include.Labels[0].Name = "a-valid-name"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestListResponseValidationCases(c *gc.C) {
	var resp = ListResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		Shards: []ListResponse_Shard{
			{
				ModRevision: 0,
				Spec: ShardSpec{
					Id:                "a invalid id",
					Sources:           []ShardSpec_Source{{Journal: "a/journal"}},
					RecoveryLogPrefix: "a/log/prefix",
					HintPrefix:        "/a/hint/prefix",
					MaxTxnDuration:    1,
				},
				Route:  pb.Route{Primary: 0},
				Status: nil,
			},
		},
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\].Spec.Id: not a valid token \(.*\)`)
	resp.Shards[0].Spec.Id = "a-valid-id"
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\]: invalid ModRevision \(0; expected > 0\)`)
	resp.Shards[0].ModRevision = 1
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\].Route: invalid Primary .*`)
	resp.Shards[0].Route.Members = []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "suffix"}}
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\]: length of Route.Members and Status are not equal \(1 vs 0\)`)
	resp.Shards[0].Status = []ReplicaStatus{{Code: ReplicaStatus_STANDBY}}

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestApplyRequestValidationCases(c *gc.C) {
	var req = ApplyRequest{
		Changes: []ApplyRequest_Change{
			{
				ExpectModRevision: -1,
				Upsert: &ShardSpec{
					Id:                "invalid id",
					Sources:           []ShardSpec_Source{{Journal: "a/journal"}},
					RecoveryLogPrefix: "a/log/prefix",
					HintPrefix:        "/a/hint/prefix",
					MaxTxnDuration:    1,
				},
				Delete: "another-id",
			},
			{
				ExpectModRevision: 0,
				Delete:            "another invalid id",
			},
			{
				ExpectModRevision: 1,
			},
		},
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: both Upsert and Delete are set \(expected exactly one\)`)
	req.Changes[0].Delete = ""
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\].Upsert.Id: not a valid token \(invalid id\)`)
	req.Changes[0].Upsert.Id = "a-valid-id"
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: invalid ExpectModRevision \(-1; expected >= 0\)`)
	req.Changes[0].ExpectModRevision = 0
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\].Delete: not a valid token \(another invalid id\)`)
	req.Changes[1].Delete = "other-valid-id"
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\]: invalid ExpectModRevision \(0; expected > 0\)`)
	req.Changes[1].ExpectModRevision = 1
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[2\]: neither Upsert nor Delete are set \(expected exactly one\)`)
	req.Changes[2].Delete = "yet-another-valid-id"

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

func badHeaderFixture() *pb.Header {
	return &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "zone", Suffix: "name"},
		Route:     pb.Route{Primary: 0, Members: []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "name"}}},
		Etcd: pb.Header_Etcd{
			ClusterId: 0, // ClusterId is invalid, but easily fixed up.
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

var _ = gc.Suite(&RPCSuite{})
