package runtime

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestDeriveStats(t *testing.T) {
	// Simulate a derivation with 4 transforms, and then publish stats for 3 of them, each with a
	// different combination of update and publish lambdas. This tests all of the conditional
	// inclusions of transforms and invocation stats.
	var derivationSpec = pf.DerivationSpec{
		Collection: pf.CollectionSpec{
			Collection: pf.Collection("test/derive"),
		},
		Transforms: []pf.TransformSpec{
			{
				Transform:     pf.Transform("transformA"),
				UpdateLambda:  &pf.LambdaSpec{},
				PublishLambda: &pf.LambdaSpec{},
				Shuffle: pf.Shuffle{
					SourceCollection: "collectionA",
				},
			},
			{
				Transform:    pf.Transform("transformB"),
				UpdateLambda: &pf.LambdaSpec{},
				Shuffle: pf.Shuffle{
					SourceCollection: "collectionA",
				},
			},
			{
				Transform:     pf.Transform("transformC"),
				PublishLambda: &pf.LambdaSpec{},
				Shuffle: pf.Shuffle{
					SourceCollection: "collectionC",
				},
			},
			{
				Transform:     pf.Transform("transformD"),
				UpdateLambda:  &pf.LambdaSpec{},
				PublishLambda: &pf.LambdaSpec{},
			},
		},
	}
	var subject = Derive{
		taskTerm: taskTerm{
			StatsFormatter: newTestFormatter("test/derive", "derivation"),
		},
		derivation: derivationSpec,
	}
	var input = pf.DeriveAPI_Stats{
		Transforms: []*pf.DeriveAPI_Stats_TransformStats{
			{
				Input: &pf.DocsAndBytes{
					Docs:  6,
					Bytes: 6666,
				},
				Publish: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  2,
						Bytes: 2222,
					},
					TotalSeconds: 2.0,
				},
				Update: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  3,
						Bytes: 3333,
					},
					TotalSeconds: 3.0,
				},
			},
			{
				Input: &pf.DocsAndBytes{
					Docs:  1,
					Bytes: 1111,
				},
				Update: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  7,
						Bytes: 7777,
					},
					TotalSeconds: 7.0,
				},
			},
			{
				Input: &pf.DocsAndBytes{
					Docs:  8,
					Bytes: 8888,
				},
				Publish: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  9,
						Bytes: 9999,
					},
					TotalSeconds: 9.0,
				},
			},
			// Last entry is nil, which would be the case if no documents were processed for the
			// transform in a given transaction.
			nil,
		},
		Registers: &pf.DeriveAPI_Stats_RegisterStats{
			Created: 4,
		},
		Output: &pf.DocsAndBytes{
			Docs:  5,
			Bytes: 5555,
		},
	}

	var actual = subject.deriveStats(&input)
	assertEventMeta(t, actual, &derivationSpec, "derivation")

	var expected = DeriveStats{
		Transforms: map[string]DeriveTransformStats{
			"transformA": {
				Source: "collectionA",
				Input: DocsAndBytes{
					Docs:  6,
					Bytes: 6666,
				},
				Publish: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  2,
						Bytes: 2222,
					},
					SecondsTotal: 2.0,
				},
				Update: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  3,
						Bytes: 3333,
					},
					SecondsTotal: 3.0,
				},
			},
			"transformB": {
				Source: "collectionA",
				Input: DocsAndBytes{
					Docs:  1,
					Bytes: 1111,
				},
				Update: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  7,
						Bytes: 7777,
					},
					SecondsTotal: 7.0,
				},
			},
			"transformC": {
				Source: "collectionC",
				Input: DocsAndBytes{
					Docs:  8,
					Bytes: 8888,
				},
				Publish: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  9,
						Bytes: 9999,
					},
					SecondsTotal: 9.0,
				},
			},
		},
		Out: DocsAndBytes{
			Docs:  5,
			Bytes: 5555,
		},
		Registers: &DeriveRegisterStats{
			CreatedTotal: 4,
		},
	}
	require.Equal(t, &expected, actual.Derive)
}
