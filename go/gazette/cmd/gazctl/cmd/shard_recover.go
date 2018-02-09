package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path"

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
	Use:   "recover [hints-locator] [local-output-path]",
	Short: "Recover contents of the indicated log hints.",
	Long: `Recover replays the recoverylog indicated by the argument hints
into a local output directory, and additionally writes recovered JSON hints
upon completion.

hints-locator may be a path to a local file ("path/to/hints") or an Etcd
key specifier ("etcd:///path/to/hints").
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var hints, tgtPath = loadHints(args[0]), args[1]

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
		var recoveredPath = path.Join(tgtPath, "recoveredHints.json")

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
