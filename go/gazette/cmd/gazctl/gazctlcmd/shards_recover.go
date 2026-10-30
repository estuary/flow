package gazctlcmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/estuary/flow/go/gazette/broker/client"
	pb "github.com/estuary/flow/go/gazette/broker/protocol"
	"github.com/estuary/flow/go/gazette/consumer"
	pc "github.com/estuary/flow/go/gazette/consumer/protocol"
	"github.com/estuary/flow/go/gazette/consumer/recoverylog"
	mbp "github.com/estuary/flow/go/gazette/mainboilerplate"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type cmdShardsRecover struct {
	ID  string `long:"id" required:"true" description:"Shard ID"`
	Dir string `long:"dir" short:"d" required:"true" description:"Directory to write the played recovery log into"`
}

func init() {
	CommandRegistry.AddCommand("shards", "recover", "Recover the latest checkpoint from shard", `
Recover the latest checkpoint of a shard by playing its recoverylog.

Given a shard name, reads the shard recovery logs and plays them using a recoverylog.Player,
writing the played logs into a chosen directory.

Examples:

# Play a recovery log into logs directory
gazctl shards recover --id=your/shard/id --dir=path/to/dir
`, &cmdShardsRecover{})
}

func (cmd *cmdShardsRecover) Execute([]string) error {
	startup(ShardsCfg.BaseConfig)

	// Install a signal handler which cancels a top-level |ctx|.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	var ctx, cancel = context.WithCancel(context.Background())
	go func() {
		<-signalCh
		cancel()
	}()

	var shardClient = ShardsCfg.Consumer.MustShardClient(ctx)
	var shardResp, err = shardClient.List(pb.WithDispatchDefault(ctx), &pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("id", cmd.ID),
		},
	}, grpc.WaitForReady(true))
	mbp.Must(err, "failed to fetch shard spec")
	if len(shardResp.Shards) != 1 {
		log.Fatal("no shard exists with the given id")
	}

	hintResp, err := consumer.FetchHints(ctx, shardClient, &pc.GetHintsRequest{
		Shard: pc.ShardID(cmd.ID),
	})
	mbp.Must(err, "failed to fetch hints for shard")

	log.WithField("hints", hintResp).Debug("fetched shard recovery hints")

	var recoveryLog = shardResp.Shards[0].Spec.RecoveryLog()
	var hints = consumer.PickFirstHints(hintResp, recoveryLog)
	var rjc = ShardsCfg.Broker.MustRoutedJournalClient(ctx)
	var ajc = client.NewAppendService(ctx, rjc)
	var player = recoverylog.NewPlayer()
	player.FinishAtWriteHead()
	err = player.Play(ctx, hints, cmd.Dir, ajc)
	mbp.Must(err, "failed to play recoverylog")

	return nil
}
