package gazctlcmd

import (
	"context"
	"fmt"

	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
)

type cmdJournalStoreHealth struct {
	Store struct {
		URL pb.FragmentStore `positional-arg-name:"STORE_URL" required:"true" description:"Fragment store URL to check health, e.g. s3://bucket/prefix/"`
	} `positional-args:"yes"`
}

func init() {
	CommandRegistry.AddCommand("journals", "store-health", "Check fragment store health", `
Check the health of a fragment store by querying the broker's FragmentStoreHealth RPC.

The command accepts a fragment store URL and reports whether the store is healthy
or provides details about any errors preventing access to the store.

Examples:

Check an S3 store:
>  gazctl journals store-health s3://my-bucket/and/prefix/

Check a Google Cloud Storage store:
>  gazctl journals store-health gs://my-bucket/and/prefix/

Check a local filesystem store:
>  gazctl journals store-health file:///var/local/data/
`, &cmdJournalStoreHealth{})
}

func (cmd *cmdJournalStoreHealth) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)

	var ctx = context.Background()
	var jc = JournalsCfg.Broker.MustJournalClient(ctx)

	var resp, err = client.FragmentStoreHealth(ctx, jc, cmd.Store.URL)
	if err != nil {
		return err
	}

	if resp.Status == pb.Status_OK {
		fmt.Printf("Fragment store %s is healthy\n", cmd.Store.URL)
		return nil
	} else if resp.Status == pb.Status_FRAGMENT_STORE_UNHEALTHY {
		return fmt.Errorf("fragment store %s is unhealthy: %s", cmd.Store.URL, resp.StoreHealthError)
	} else {
		return fmt.Errorf("fragment store %s health check failed with status: %s", cmd.Store.URL, resp.Status)
	}
}
