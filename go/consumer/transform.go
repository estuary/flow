package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/estuary/flow/go/labels"
	pb "go.gazette.dev/core/broker/protocol"
)

type transform struct {
	id               int
	sourceName       string
	sourcePartitions pb.LabelSelector
	shuffleKey       []string
	shuffleBroadcast int
	shuffleChoose    int
}

func loadTransforms(db *sql.DB, derivation string) ([]transform, error) {
	var transforms []transform

	var rows, err = db.Query(`
	SELECT
		transform_id,
		source_name,
		source_partitions_json,
		shuffle_key_json,
		shuffle_broadcast,
		shuffle_choose
	FROM transform_details
		WHERE derivation_name = ?`,
		derivation,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read transforms from catalog: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var transform transform

		// Structure is from view 'transform_source_partitions_json' in catalog.sql
		var partitionsFlat []struct {
			Field, Value string
			Exclude      bool
		}
		if err = rows.Scan(
			&transform.id,
			&transform.sourceName,
			scanJSON{&partitionsFlat},
			scanJSON{&transform.shuffleKey},
			&transform.shuffleBroadcast,
			&transform.shuffleChoose,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tranform from catalog: %w", err)
		}

		transform.sourcePartitions.Include.AddValue(labels.Collection, transform.sourceName)
		for _, f := range partitionsFlat {
			if f.Exclude {
				transform.sourcePartitions.Exclude.AddValue(encodePartitionToLabel(f.Field, f.Value))
			} else {
				transform.sourcePartitions.Include.AddValue(encodePartitionToLabel(f.Field, f.Value))
			}
		}
	}

	if len(transforms) == 0 {
		return nil, fmt.Errorf("read no transforms for derivation %v", derivation)
	}
	return transforms, nil
}

func encodePartitionToLabel(field string, valueJSON string) (name, value string) {
	name = labels.FieldPrefix + field
	if l := len(valueJSON); l != 0 && valueJSON[0] == '"' {
		valueJSON = valueJSON[1 : l-1] // Strip quotes wrapping string.
	}
	value = url.QueryEscape(valueJSON)
	return
}

type scanJSON struct {
	v interface{}
}

func (j scanJSON) Scan(value interface{}) error {
	var b, ok = value.([]byte)
	if !ok {
		return fmt.Errorf("scaning json: %v is not a []byte", value)
	}
	return json.Unmarshal(b, j.v)
}
