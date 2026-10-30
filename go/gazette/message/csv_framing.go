package message

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"

	"github.com/google/uuid"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/labels"
)

// CSVFramable is the interface of a Frameable required by a CSV Framing.
type CSVFrameable interface {
	//  MarshalCSV returns CSV records describing of the message.
	MarshalCSV() ([]string, error)
	// UnmarshalCSV applies the records to unmarshal the message
	// from its CSV description. It must copy the []string records if it
	// wishes to retain them after returning.
	UnmarshalCSV([]string) error
}

// CSVRecord is a minimal implementation of CSVFrameable and Message.
// It requires that the first field is a string-encoded UUID.
type CSVRecord []string

// MarshalCSV returns the CSVRecord directly.
func (r CSVRecord) MarshalCSV() ([]string, error) { return r, nil }

// UnmarshalCSV copies the []string to this CSVRecord,
// and verifies the first column parses as a UUID.
func (r *CSVRecord) UnmarshalCSV(fields []string) error {
	if len(fields) == 0 {
		return errors.New("fields are empty")
	}
	if _, err := uuid.Parse(fields[0]); err != nil {
		return err
	}
	*r = append((*r)[:0], fields...)
	return nil
}

func (r CSVRecord) GetUUID() UUID                         { return uuid.MustParse(r[0]) }
func (r CSVRecord) SetUUID(uuid UUID)                     { r[0] = uuid.String() }
func (r CSVRecord) NewAcknowledgement(pb.Journal) Message { return make(CSVRecord, len(r)) }

type csvFraming struct{}

func (csvFraming) ContentType() string { return labels.ContentType_CSV }

func (csvFraming) Marshal(f Frameable, bw *bufio.Writer) error {
	var cf, ok = f.(CSVFrameable)
	if !ok {
		return fmt.Errorf("%#v is not a CSVFrameable", f)
	} else if records, err := cf.MarshalCSV(); err != nil {
		return err
	} else {
		return csv.NewWriter(bw).Write(records) // Marshals directly to |bw|.
	}
}

func (csvFraming) NewUnmarshalFunc(r *bufio.Reader) UnmarshalFunc {
	var cr = csv.NewReader(r)
	cr.ReuseRecord = true

	return func(f Frameable) error {
		var cf, ok = f.(CSVFrameable)
		if !ok {
			return fmt.Errorf("%#v is not a CSVFrameable", f)
		}

		var records, err = cr.Read()
		if err != nil {
			return err
		}
		return cf.UnmarshalCSV(records)
	}
}

func init() { RegisterFraming(new(csvFraming)) }
