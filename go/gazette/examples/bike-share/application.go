// Package bike_share implements a Gazette consumer application which processes
// and serves streaming Citi Bike system data. It indexes a window of recent
// rides for each bike, serves a simple history API, and detects long graph
// cycles as they're completed by individual bikes.
package bike_share

import (
	"database/sql"
	"io"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	store_sqlite "go.gazette.dev/core/consumer/store-sqlite"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// Cycle describes a graph cycle completed by a bike.
type Cycle struct {
	UUID   message.UUID
	BikeID int
	Steps  []CycleStep
}

// CycleStep is a path step of a bike's graph cycle.
type CycleStep struct {
	Time    time.Time
	Station string
}

// GetUUID returns the Cycle's UUID.
func (c *Cycle) GetUUID() message.UUID { return c.UUID }

// SetUUID sets the Cycle's UUID. It's called by message.Publisher.
func (c *Cycle) SetUUID(uuid message.UUID) { c.UUID = uuid }

// NewAcknowledgement returns a new & zero-valued Cycle. message.Publisher uses
// it to build a message which acknowledge other Cycle messages previously published.
func (c *Cycle) NewAcknowledgement(pb.Journal) message.Message { return new(Cycle) }

// Application is a consumer framework application which finds cycles in
// bike-share data. It implements the consumer.Application interface
// as well as runconsumer.Application, which extends
// consumer.Application with configuration parsing and initialization.
type Application struct {
	runconsumer.BaseConfig
	DBAddr string `long:"postgres" description:"Database connection string" default:"host=/var/run/postgresql" required:"true"`

	svc                                      *consumer.Service
	db                                       *sql.DB
	insert, window, queryCycle, queryHistory *sql.Stmt

	rideMapping  message.MappingFunc
	cycleMapping message.MappingFunc
}

// NewConfig returns the Config struct of our Application, parse-able with `github.com/jessevdk/go-flags`.
// In this case our Application's type is also its config.
func (app *Application) NewConfig() runconsumer.Config { return app }

// InitApplication initializes dynamic mappings of bike-share data types to
// responsible partitions, opens the configured database, prepares DB statements
// for future use, and registers a simple bike-share history HTTP API.
func (app *Application) InitApplication(args runconsumer.InitArgs) (err error) {
	// Initialize a mapping from a bike ID to it's responsible Ride partition.
	// This matches the mapping used by gazctl when loading records.
	app.rideMapping = buildMapping("bike_share.Ride",
		func(m message.Mappable, w io.Writer) {
			_, _ = w.Write(m.([]byte))
		}, args)

	// Initialize a mapping from bike ID to it's responsible Cycle partition.
	app.cycleMapping = buildMapping("bike_share.Cycle",
		func(m message.Mappable, w io.Writer) {
			_, _ = w.Write([]byte(strconv.Itoa(m.(*Cycle).BikeID)))
		}, args)

	if app.DBAddr == "" {
		// Pass.
	} else if app.db, err = sql.Open("postgres", app.DBAddr); err != nil {
		return
	} else if app.insert, err = app.db.PrepareContext(args.Context, InsertStmt); err != nil {
		return
	} else if app.window, err = app.db.PrepareContext(args.Context, WindowStmt); err != nil {
		return
	} else if app.queryCycle, err = app.db.PrepareContext(args.Context, QueryCycleStmt); err != nil {
		return
	} else if app.queryHistory, err = app.db.PrepareContext(args.Context, QueryHistoryStmt); err != nil {
		return
	}

	// The provided Server can be used to register HTTP or gRPC APIs. If needed,
	// we can also support additional protocols by registering a suitable matcher
	// with the Server's CMux listener. All protocols are multiplexed from the
	// single server port.
	app.svc = args.Service
	args.Server.HTTPMux.Handle("/api/bikes", http.HandlerFunc(app.ServeBikeHistory))
	return
}

// NewMessage returns an instance of the appropriate message type for decoding
// from the given journal. For this use-case, we use the provided
// message.CSVRecord type.
func (Application) NewMessage(*pb.JournalSpec) (message.Message, error) {
	return new(message.CSVRecord), nil
}

// NewStore instantiates either a SQLStore using the remote DB or, if the Shard has
// a recovery log, an embedded store_sqlite.Store.
func (app *Application) NewStore(_ consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	if rec == nil {
		return consumer.NewSQLStore(app.db), nil
	}

	var store, err = store_sqlite.NewStore(rec)
	if err != nil {
		return nil, err
	}
	return store, store.Open(CreateTableStmt, InsertStmt, WindowStmt, QueryCycleStmt, QueryHistoryStmt)
}

// ConsumeMessage inserts a CSVRecord of bike-share ride data into the store and
// queries for a now-completed cycle of the record's bike ID. If matched,
// it publishes a Cycle event.
func (app *Application) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	var err error

	// Dispatch to a sql.Tx and Stmts based on the Store implementation.
	var txn *sql.Tx
	var insert, window, query *sql.Stmt

	switch s := store.(type) {
	case *consumer.SQLStore:
		if txn, err = s.Transaction(shard.Context(), nil); err != nil {
			return err
		}
		insert, window, query = app.insert, app.window, app.queryCycle

	case *store_sqlite.Store:
		if txn, err = s.Transaction(shard.Context(), nil); err != nil {
			return err
		}
		insert, window, query = s.Stmts[0], s.Stmts[1], s.Stmts[2]

	default:
		panic("not reached")
	}

	var record = *env.Message.(*message.CSVRecord)

	// Insert the record into the "rides" table.
	if _, err = txn.Stmt(insert).Exec(strSliceToIface(record)...); err != nil {
		return err
	}
	// Window to the last 20 rides of this bike.
	if _, err = txn.Stmt(window).Exec(record[12], 20); err != nil {
		return err
	}
	// Query for a completed path of this bike having length >= 10.
	// The queryCycle will return zero rows, or a row for each step of a complete path.
	rows, err := txn.Stmt(query).Query(record[12], 10)
	if err != nil {
		return err
	}

	// Build a Cycle event (if matched), and publish.
	var cycle Cycle
	for rows.Next() {
		var step CycleStep

		if err = rows.Scan(&cycle.BikeID, &step.Time, &step.Station); err != nil {
			return err
		}
		cycle.Steps = append(cycle.Steps, step)
	}
	if rows.Err() != nil {
		return rows.Err()
	}

	// Publish an uncommitted message to maintain exactly-once semantics.
	// The consumer framework will automatically publish a suitable
	// acknowledgement upon the successful commit of the consumer transaction
	// we're currently within.
	if len(cycle.Steps) == 0 {
		// Not matched.
	} else if _, err = pub.PublishUncommitted(app.cycleMapping, &cycle); err != nil {
		return err
	}
	return nil
}

// FinalizeTxn is a no-op for this Application. If the application kept in-memory-only
// updates while consuming messages, FinalizeTxn would be the time for it to flush
// them out.
func (Application) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error { return nil }

// buildMapping starts a long-lived watch of journals labeled with the
// given message type, and returns a mapping to journals using modulo arithmetic
// over the provided MappingKeyFunc.
func buildMapping(msgType string, fn message.MappingKeyFunc, args runconsumer.InitArgs) message.MappingFunc {
	var parts = client.NewWatchedList(args.Context, args.Service.Journals,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet(labels.MessageType, msgType),
			},
		},
		nil,
	)
	mbp.Must(<-parts.UpdateCh(), "failed to fetch partitions")

	for len(parts.List().Journals) == 0 {
		log.WithField("msg", msgType).Info("waiting for partitions to be created")
		<-parts.UpdateCh()
	}
	return message.ModuloMapping(fn, parts.List)
}

// strSliceToIFace maps []string => []interface{}
func strSliceToIface(r []string) []interface{} {
	var out = make([]interface{}, len(r))
	for i := range r {
		out[i] = r[i]
	}
	return out
}
