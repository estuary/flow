package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/recoverylog"
)

var shardCmd = &cobra.Command{
	Use:   "shard",
	Short: "Commands for working with a gazette consumer shard",
}

var shardRecoverCmd = &cobra.Command{
	Use:   "recover [etcd-hints-path] [local-output-path]",
	Short: "Recover contents of the indicated log hints.",
	Long: `Recover replays the recoverylog indicated by the argument Etcd hints
path into a local output directory, and additionally writes recovered JSON hints
upon completion.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var hintsPath, tgtPath = args[0], args[1]

		var hintsResp, err = etcd.NewKeysAPI(etcdClient()).Get(context.Background(), hintsPath, nil)
		if err != nil {
			log.WithField("err", err).Fatal("failed to read hints from Etcd")
		}

		var hints recoverylog.FSMHints
		if err = json.Unmarshal([]byte(hintsResp.Node.Value), &hints); err != nil {
			log.WithFields(log.Fields{"err": err, "resp": *hintsResp}).
				Fatal("failed to unmarshal hints")
		}

		player, err := recoverylog.NewPlayer(hints, tgtPath)
		if err != nil {
			log.WithField("err", err).Fatal("preparing playback")
		}

		go func() {
			if err := player.PlayContext(
				context.Background(),
				struct {
					*gazette.Client
					*gazette.WriteService
				}{gazetteClient(), writeService()},
			); err != nil {
				log.WithField("err", err).Fatal("shard playback failed")
			}
		}()

		var fsm = player.FinishAtWriteHead()
		if fsm == nil {
			return
		}

		// Write recovered hints under |tgtPath|.
		var recoveredPath = path.Join(tgtPath, path.Base(hintsPath)+".recoveredHints.json")

		fout, err := os.Create(recoveredPath)
		if err == nil {
			err = json.NewEncoder(fout).Encode(fsm.BuildHints())
		}
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": recoveredPath}).Fatal("failed to write recovered hints")
		} else {
			log.WithField("path", recoveredPath).Info("wrote recovered hints")
		}
	},
}

func init() {
	rootCmd.AddCommand(shardCmd)

	shardCmd.AddCommand(shardRecoverCmd)
}
