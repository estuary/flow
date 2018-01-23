package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var createCmd = &cobra.Command{
	Use:   "create [journal name] [journal name] ...",
	Short: "Create new gazette journals",
	Long: `Create one or more new gazette journals.

Example: gazctl create examples/a-journal/one examples/a-journal/two
This creates journals examples/a-journal/one & examples/a-journal/two.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}

		for i := range args {
			var name = journal.Name(args[i])

			userConfirms(fmt.Sprintf(
				"WARNING: Really create %s? This cannot be undone.", name.String()))

			if err := gazetteClient().Create(name); err != nil {
				log.WithField("err", err).Fatal("failed to create journal")
			} else {
				log.WithField("name", name).Info("created journal")
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	createCmd.Flags().BoolVarP(&defaultYes, "yes", "y", false, "Create without asking for confirmation.")
}
