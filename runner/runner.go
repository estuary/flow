package runner

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// config configures the ping-pong application.
type config struct {
	runconsumer.BaseConfig

	// PingPong application flags.
	PingPong struct {
		Players int           `long:"players" default:"100" description:"Number of ping-pong players" env:"PLAYERS"`
		Period  time.Duration `long:"period" default:"1s" description:"Average period between game starts" env:"PERIOD"`
	} `group:"ping-pong" namespace:"ping-pong" env-namespace:"PING_PONG"`
}

type Document struct {
	// ???
}

func (d *Document) GetUUID() (uuid message.UUID) { return }

// SetUUID sets the Gazette UUID of a Volley. It implements message.Message.
func (c *Document) SetUUID(uuid message.UUID) {}

// NewAcknowledgement returns a new Volley. It implements message.Message.
func (c *Document) NewAcknowledgement(pb.Journal) message.Message { return new(Document) }

type App struct {
	cfg     config
	mapping message.MappingFunc
}

func (p *App) NewStore(_ consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	return consumer.NewJSONFileStore(rec, new(struct{}))
}

func (p *App) NewMessage(*pb.JournalSpec) (message.Message, error) {
	return new(Document), nil
}

// ConsumeMessage receives Volleys, and returns them to a randomly selected player.
func (p *App) ConsumeMessage(_ consumer.Shard, _ consumer.Store, env message.Envelope, pub *message.Publisher) error {
	var recv = env.Message.(*Document)

	if message.GetFlags(recv.GetUUID()) == message.Flag_ACK_TXN {
		return nil // Ignore transaction acknowledgement messages.
	}
	return nil
}

// FinalizeTxn is a no-op, as we have no deferred work to finish before the transaction closes.
func (p *App) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error {
	return nil // No-op.
}

// NewConfig returns a new config instance.
func (p *App) NewConfig() runconsumer.Config { return new(config) }

// InitApplication validates configuration and initializes the ping-pong application.
func (p *App) InitApplication(args runconsumer.InitArgs) error {
	p.cfg = *args.Config.(*config)

	if p.cfg.PingPong.Players <= 2 {
		return errors.New("Players must be >= 2")
	} else if p.cfg.PingPong.Period < 0 {
		return errors.New("ServePeriod must be >= 0")
	}

	// Select all journals having message-type "Volley".
	var partitions, err = client.NewPolledList(args.Context, args.Service.Journals, 30*time.Second,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet(labels.MessageType, "ping_pong.Volley"),
			},
		})
	if err != nil {
		return err
	}
	// Map Volley messages to partitions using a modulo-hash of the "To" field.
	p.mapping = message.ModuloMapping(func(m message.Mappable, w io.Writer) {
		_, _ = w.Write([]byte(fmt.Sprintf("%x", m.(*Volley).To)))
	}, partitions.List)

	if p.cfg.PingPong.Period != 0 {
		var as = client.NewAppendService(args.Context, args.Service.Journals)
		go startGames(p.mapping, message.NewPublisher(as, nil), p.cfg)
	}
	return nil
}
