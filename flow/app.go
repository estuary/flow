package flow

import (
	"database/sql"
	"fmt"

	"github.com/estuary/proj/bridge"
	"github.com/estuary/proj/labels"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// config configures the Flow application.
type config struct {
	runconsumer.BaseConfig

	// Flow application flags.
	Flow struct {
		Catalog string `long:"catalog" description:"Path to catalog database" env:"CATALOG"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Flow implements the Estuary Flow consumer.Application.
type Flow struct {
	cfg     config
	catalog *sql.DB
	builder *bridge.MsgBuilder
}

// NewStore returns something??
func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var derive = shard.Spec().LabelSet.ValueOf(labels.Derive)
	if derive == "" {
		return nil, fmt.Errorf("expected label %q", labels.Derive)
	}

	// Map |derive| to a derivations.collection_id.
	//  - Extract "durable" or "ephemeral" (fixed_shards != 0)
	//  - Extract lambda boostrap_id.

	// Recover rocksDB.
	// Recover wrapped SQLiteStore.

	//  - Invoke bootstrap somehow?

	return nil, nil
}

// NewMessage builds bridge.Message instances.
func (f *Flow) NewMessage(spec *pb.JournalSpec) (message.Message, error) {
	return f.builder.Build(spec)
}

// ConsumeMessage receives Volleys, and returns them to a randomly selected player.
func (f *Flow) ConsumeMessage(_ consumer.Shard, _ consumer.Store, env message.Envelope, pub *message.Publisher) error {
	var msg = env.Message.(bridge.Message)

	if message.GetFlags(msg.GetUUID()) == message.Flag_ACK_TXN {
		return nil // Ignore transaction acknowledgement messages.
	}
	return nil
}

// FinalizeTxn is a no-op, as we have no deferred work to finish before the transaction closes.
func (f *Flow) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error {
	return nil // No-op.
}

// NewConfig returns a new config instance.
func (f *Flow) NewConfig() runconsumer.Config { return new(config) }

// InitApplication validates configuration and initializes the ping-pong application.
func (f *Flow) InitApplication(args runconsumer.InitArgs) error {
	f.cfg = *args.Config.(*config)

	var err error
	var dbURL = "file://" + f.cfg.Flow.Catalog + "?immutable=true"

	if f.catalog, err = sql.Open("sqlite3", dbURL); err != nil {
		return fmt.Errorf("opening catalog database %v: %w", dbURL, err)
	}
	return nil
}

func main() { runconsumer.Main(new(Flow)) }
