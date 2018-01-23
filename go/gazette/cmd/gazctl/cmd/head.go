package cmd

import (
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var headCmd = &cobra.Command{
	Use:   "head [journal name] [journal name] ...",
	Short: "Print gazette journal metadata",
	Long: `Print metadata of a gazette journal, such as its route and current write head, as JSON.

Example: gazctl head examples/a-journal/one examples/a-journal/two`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}

		var out = json.NewEncoder(os.Stdout)

		for i := range args {
			var args = journal.ReadArgs{
				Journal: journal.Name(args[i]),
				Offset:  readOffset,
			}

			var result journal.ReadResult
			if result, _ = gazetteClient().Head(args); result.Error != nil {
				log.WithFields(log.Fields{"err": result.Error, "name": args.Journal}).Fatal("failed to HEAD journal")
			}

			if err := out.Encode(struct {
				Name   journal.Name
				Result journal.ReadResult
			}{args.Journal, result}); err != nil {
				log.WithFields(log.Fields{"err": err, "name": args.Journal}).Fatal("failed to write HEAD output")
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(headCmd)

	headCmd.Flags().Int64VarP(&readOffset, "offset", "c", 0,
		"Byte offset to HEAD at, or -1 for the current write-head")
}
