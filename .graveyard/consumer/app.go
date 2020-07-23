package consumer

import (
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/labels"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// config configures the Flow application.
type config struct {
	runconsumer.BaseConfig

	// Flow application flags.
	Flow struct {
		Fizzle string `long:"fizzle" description:"Fizzle bizzle" env:"FIZZLE"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Flow implements the Estuary Flow consumer.Application.
type Flow struct {
	cfg     config
	msgMeta RawJSONMeta
}

var _ runconsumer.Application = (*Flow)(nil)

// StartReadingMessages spawns a read-loop to read source collections and partitions thereof.
func (f *Flow) StartReadingMessages(shard consumer.Shard, checkpoint pc.Checkpoint, ch chan<- EnvelopeOrError) error {
	catalogURL, err := getLabel(shard.Spec(), labels.CatalogURL)
	if err == nil {
		return err
	}
	catalogURL += "?immutable=true"

	db, err := sql.Open("sqlite3", catalogURL)
	if err != nil {
		return fmt.Errorf("opening catalog database %v: %w", catalogURL, err)
	}
	defer db.Close()

	return startReadingMessages(shard, checkpoint, ch)
}

// NewStore starts and returns a new derive worker, which implements the consumer.Store interface.
func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	return newWorker(shard, rec)
}

// NewMessage returns a new RawJSONMessage instances.
func (f *Flow) NewMessage(spec *pb.JournalSpec) (message.Message, error) {
	return RawJSONMessage{Meta: &f.msgMeta}, nil
}

// ConsumeMessage dispatches RawJSONMessages to the worker.
func (f *Flow) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	return store.(*worker).consumeMessage(shard, env, pub)
}

// FinalizeTxn flushes the derive-worker transaction.
func (f *Flow) FinalizeTxn(_ consumer.Shard, store consumer.Store, _ *message.Publisher) error {
	return store.(*worker).finalizeTxn()
}

// NewConfig returns a new config instance.
func (f *Flow) NewConfig() runconsumer.Config { return new(config) }

// InitApplication validates configuration and initializes the Flow application.
func (f *Flow) InitApplication(args runconsumer.InitArgs) error {
	f.cfg = *args.Config.(*config)
	f.msgMeta = RawJSONMeta{
		UUIDPath:    []string{"_meta", "uuid"},
		ACKTemplate: []byte(`{"_meta":{"uuid":"` + placeholderUUID + `"}}`),
	}

	/*
		var err error
		var dbURL = "file://" + f.cfg.Flow.Catalog + "?immutable=true"

		if f.catalog, err = sql.Open("sqlite3", dbURL); err != nil {
			return fmt.Errorf("opening catalog database %v: %w", dbURL, err)
		}
	*/
	return nil
}

func main() { runconsumer.Main(new(Flow)) }

func getLabel(spec *pc.ShardSpec, label string) (string, error) {
	if values := spec.LabelSet.ValuesOf(label); len(values) != 0 {
		return "", fmt.Errorf("expected single label %q (got %v)", label, values)
	} else {
		return values[0], nil
	}
}
