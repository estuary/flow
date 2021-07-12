package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJournalsFragments struct {
	Selector string        `long:"selector" short:"l" description:"Label Selector query to filter on"`
	Format   string        `long:"format" short:"o" choice:"table" choice:"json" choice:"proto" default:"table" description:"Output format"`
	FromUnix int64         `long:"from" description:"Restrict to fragments created at or after this time, in unix seconds since epoch"`
	ToUnix   int64         `long:"to" description:"Restrict to fragments created before this time, in unix seconds since epoch"`
	SigTTL   time.Duration `long:"url-ttl" default:"0s" description:"Provide a signed GET URL with the given TTL"`
}

func init() {
	_ = mustAddCmd(cmdJournals, "fragments", "List journal fragments", `
List fragments of selected journals.

A label --selector is required, and determines the set of journals for which
fragments are listed. See "journals list --help" for details and examples of
using journal selectors. 

Use --from and/or --to to retrieve fragments persisted within the given time
range. Note that each fragment is evaluated based on its modification
timestamp as supplied by its backing fragment store. Usually this will be the
time at which the fragment was uploaded to the store, but may not be if
another process has modified or touched the fragment (Gazette itself will never
modify a fragment once written). --from and --to are given in Unix seconds since
the epoch. Use the 'date' tool to convert humanized timestamps to epoch values.

If --url-ttl, the broker will generate and return a signed GET URL having the
given TTL, suitable for directly reading the fragment from the backing store.

Results can be output in a variety of --format options:
json: Prints Fragments encoded as JSON, one per line.
proto: Prints Fragments and response headers in protobuf text format.
table: Prints as a humanized table.

Combining --from, --to, and --url-ttl enables this command to generate inputs for
regularly-run batch processing pipelines. For example, a cron job running at ten
past the hour would fetch fragments persisted between the beginning and end of
the last hour with an accompanying signed URL. That fragment list becomes input
to an hourly batch pipeline run, which can directly read journal data from URLs
without consulting brokers (or even being aware of them). 

See also the 'flush_interval' JournalSpec field, which can be used to bound the
maximum delay of a record being written to a journal, vs that same record being
persisted with its fragment to a backing store. Note that a fragment's time is
an upper-bound on the append time of all contained records, and a fragment
persisted at 4:01pm may contain records from 3:59pm. A useful pattern is to
extend the queried range slightly (eg from 3:00-4:05pm), and then filter on
record timestamps to the precise desired range (of 3:00-4:00pm).

Examples:

# List fragments of a journal in a formatted table:
gazctl journals fragments -l name=my/journal

# List fragments created in the last hour in prototext format, including a signed URL.
gazctl journals fragments -l name=my/journal --url-ttl 1m --from $(date -d "1 hour ago" '+%s') --format proto

# List fragments of journals matching my-label which were persisted between 3:00AM
# and 4:05AM today with accompanying signed URL, output as JSON.
gazctl journals fragments -l my-label --format json --url-ttl 1h --from $(date -d 3AM '+%s') --to $(date -d 4:05AM '+%s') --format json
`, &cmdJournalsFragments{})
}

func (cmd *cmdJournalsFragments) Execute([]string) error {
	startup()

	// Evaluate journal selector and map to a journal list.
	var err error
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var ctx = context.Background()
	var rjc = journalsCfg.Broker.MustRoutedJournalClient(ctx)
	listResponse, err := client.ListAllJournals(ctx, rjc, listRequest)
	mbp.Must(err, "failed to list journals")

	// Fetch fragments of selected journals in parallel.
	var wg sync.WaitGroup
	var responses = make([]*pb.FragmentsResponse, len(listResponse.Journals))

	for i := range listResponse.Journals {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var fragRequest = pb.FragmentsRequest{
				Journal:      listResponse.Journals[i].Spec.Name,
				BeginModTime: cmd.FromUnix,
				EndModTime:   cmd.ToUnix,
			}
			if cmd.SigTTL != 0 {
				fragRequest.SignatureTTL = &cmd.SigTTL
			}

			responses[i], err = client.ListAllFragments(ctx, rjc, fragRequest)
			mbp.Must(err, "failed to fetch fragments", "request", fragRequest)
		}(i)
	}
	wg.Wait()

	switch cmd.Format {
	case "table":
		cmd.outputTable(responses)
	case "json":
		var enc = json.NewEncoder(os.Stdout)
		for _, r := range responses {
			for _, f := range r.Fragments {
				mbp.Must(enc.Encode(f), "failed to encode to json")
			}
		}
	case "proto":
		for _, r := range responses {
			mbp.Must(proto.MarshalText(os.Stdout, r), "failed to encode to prototext")
		}
	}
	return nil
}

func (cmd *cmdJournalsFragments) outputTable(responses []*pb.FragmentsResponse) {
	var table = tablewriter.NewWriter(os.Stdout)

	var headers = []string{"Journal", "Offset", "Length", "Persisted", "SHA1", "Compression"}
	if cmd.SigTTL != 0 {
		headers = append(headers, "URL")
	}
	table.SetHeader(headers)

	for _, r := range responses {
		for _, f := range r.Fragments {
			var sum = f.Spec.Sum.ToDigest()
			var modTime string

			if f.Spec.ModTime != 0 {
				modTime = humanize.Time(time.Unix(f.Spec.ModTime, 0))
			}
			var row = []string{
				f.Spec.Journal.String(),
				fmt.Sprintf("%d", f.Spec.Begin),
				humanize.IBytes(uint64(f.Spec.End - f.Spec.Begin)),
				modTime,
				hex.EncodeToString(sum[:6]) + "...",
				f.Spec.CompressionCodec.String(),
			}
			if cmd.SigTTL != 0 {
				row = append(row, f.SignedUrl)
			}
			table.Append(row)
		}
	}
	table.Render()
}
