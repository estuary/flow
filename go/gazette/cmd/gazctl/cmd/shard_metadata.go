package cmd

import (
	"fmt"

	"github.com/LiveRamp/gazette/pkg/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	rocks "github.com/tecbot/gorocksdb"
)

var shardMetadata = &cobra.Command{
	Use:   "metadata [local-db-path]",
	Short: "Print metadata about a recovered consumer shard.",
	Long: `Metadata prints metadata about a recovered shard database, such as estimated
keys, live vs total data, consumer offsets, and level statistics.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var dbPath = args[0]

		var opts = rocks.NewDefaultOptions()
		if plugin := consumerPlugin(); plugin != nil {
			if initer, _ := plugin.(consumer.OptionsIniter); initer != nil {
				initer.InitOptions(opts)
			}
		}

		var db, err = rocks.OpenDbForReadOnly(opts, dbPath, true)
		if err != nil {
			log.WithFields(log.Fields{"path": dbPath, "err": err}).Fatal("failed to open RocksDB")
		}
		defer db.Close()

		var ro = rocks.NewDefaultReadOptions()
		ro.SetFillCache(false)
		defer ro.Destroy()

		offsets, err := consumer.LoadOffsetsFromDB(db, ro)
		if err != nil {
			log.WithFields(log.Fields{"path": dbPath, "err": err}).Fatal("failed to load consumer offsets")
		}

		fmt.Printf("Consumer Offsets:    %v\n", offsets)
		fmt.Printf("Estimated Keys:      %s\n", db.GetProperty("rocksdb.estimate-num-keys"))
		fmt.Printf("Total Data:          %s\n", db.GetProperty("rocksdb.total-sst-files-size"))
		fmt.Printf("Estimated Live Data: %s\n", db.GetProperty("rocksdb.estimate-live-data-size"))
		fmt.Printf("\nLevel Stats:\n%s\n", db.GetProperty("rocksdb.levelstats"))
	},
}

func init() {
	shardCmd.AddCommand(shardMetadata)
}
