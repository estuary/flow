package cmd

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var appendCmd = &cobra.Command{
	Use:   "append [journal name] [source-file-one] [source-file-two] ...",
	Short: "Atomically append content of one or more files to a gazette journal",
	Long: `Append writes the provided files to a gazette journal. File contents are written in argument
order, and each file append is atomic (all-or-nothing). When using this command, you should take the time
to be sure this is actually what you want to do and that the contents of the file are consistent with other
data in the journal. For example, if a journal contains CSV data, you probably only want to append CSV
data in the same format. Same for JSON, protobuf, etc.

Example: gazctl append example/journal/name localFileOne localFileTwo
This appends the contents of localFileOne & Two to example/journal/name.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var name, srcPaths = journal.Name(args[0]), args[1:]

		for _, p := range srcPaths {
			var file, err = os.Open(p)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "path": p}).Fatal("opening file")
			}
			userConfirms(fmt.Sprintf("WARNING: Really append %s to %s? This cannot be undone.", p, name))

			if result := gazetteClient().Put(journal.AppendArgs{
				Journal: name,
				Content: file,
			}); result.Error != nil {
				log.WithFields(log.Fields{"result": result, "path": p}).Fatal("failed to append to journal")
			} else {
				log.WithFields(log.Fields{"writeHead": result.WriteHead, "path": p}).Info("file appended to journal")
			}

			file.Close()
		}
	},
}

func userConfirms(message string) {
	if defaultYes {
		return
	}
	fmt.Println(message)
	fmt.Print("Confirm (y/N): ")

	var response string
	fmt.Scanln(&response)

	for _, opt := range []string{"y", "yes"} {
		if strings.ToLower(response) == opt {
			return
		}
	}
	log.Fatal("aborted by user")
}

var defaultYes bool

func init() {
	rootCmd.AddCommand(appendCmd)

	appendCmd.Flags().BoolVarP(&defaultYes, "yes", "y", false, "Append without asking for confirmation.")
}
