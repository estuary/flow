package cmd

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var readCmd = &cobra.Command{
	Use:   "read [journal name]",
	Short: "Read the contents of a gazette journal",
	Long: `Read and output the contents of a gazette journal.
Reads may be blocking / tailing (--block) and may begin from any non-zero
offset (--offset), or -1 (which reads from the current journal head).

Example: gazctl cat examples/a-journal
This reads journal content from byte-offset zero, through to the current write head.

Example: gazctl cat examples/a-journal --offset 1234 --block 1m
This reads journal content from byte-offset 1234, and blocks one minute to read
new content as it is appended.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}

		var mark = journal.Mark{
			Journal: journal.Name(args[0]),
			Offset:  readOffset,
		}

		var ctx = context.Background()
		if readTimeout != 0 {
			ctx, _ = context.WithTimeout(ctx, readTimeout)
		}

		var reader = journal.NewRetryReaderContext(ctx, mark, gazetteClient())
		reader.Blocking = (readTimeout != 0)

		var br = bufio.NewReader(reader)
		if _, err := br.Peek(1); err == nil {
			log.WithFields(log.Fields{"mark": reader.AdjustedMark(br)}).Info("reading from mark")
		}
		if _, err := io.Copy(os.Stdout, br); err != nil && err != journal.ErrNotYetAvailable {
			log.WithField("err", err).Fatal("failed to read journal data")
		}
	},
}

var (
	readOffset  int64
	readTimeout time.Duration
)

func init() {
	rootCmd.AddCommand(readCmd)

	readCmd.Flags().Int64VarP(&readOffset, "offset", "c", 0,
		"Byte offset to begin reading from, or -1 for the current write-head")
	readCmd.Flags().DurationVarP(&readTimeout, "block", "t", 0,
		"Total duration to block for, reading ongoing journal appends. Zero (default) does not block")
}
