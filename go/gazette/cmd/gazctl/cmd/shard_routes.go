package cmd

import (
	"os"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/consumer"
)

var shardRoutes = &cobra.Command{
	Use:   "routes [http://grpc-endpoint:grpc-port]",
	Short: "Prints current routes of the running consumer.",
	Long:  `Routes queries a consumer supporting the Consumer GRPC API and prints current routing metadata.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}

		var cc, err = consumer.NewClient(args[0])
		if err != nil {
			log.WithField("err", err).Fatal("failed to create consumer client")
		}

		var state = cc.State()
		if err := proto.MarshalText(os.Stdout, &state); err != nil {
			log.WithField("err", err).Fatal("failed to encode consumer state")
		}
	},
}

func init() {
	shardCmd.AddCommand(shardRoutes)
}
