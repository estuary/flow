package flow

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
)

// Catalog is a catalog database which has been fetched to a local file.
type catalog struct {
	dbPath string
	db     *sql.DB
}

func newCatalog(url, tempDir string) (*catalog, error) {
	// Download catalog from URL to a local file.
	var dbFile, err = ioutil.TempFile(tempDir, "catalog-db")
	if err != nil {
		return nil, fmt.Errorf("failed to create local DB tempfile: %w", err)
	}
	var dbPath = dbFile.Name()

	if resp, err := http.Get(url); err != nil {
		return nil, fmt.Errorf("failed to request catalog URL: %w", err)
	} else if _, err = io.Copy(dbFile, resp.Body); err != nil {
		return nil, fmt.Errorf("failed to copy catalog to local file: %w", err)
	} else if err = resp.Body.Close(); err != nil {
		return nil, fmt.Errorf("failed to close catalog response: %w", err)
	} else if err = dbFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close local catalog file: %w", err)
	}

	db, err := sql.Open("sqlite3", "file://"+dbPath+"?immutable=true")
	if err != nil {
		return nil, fmt.Errorf("opening catalog database %v: %w", dbPath, err)
	}

	return &catalog{
		dbPath: dbPath,
		db:     db,
	}, nil
}

func (c *catalog) LocalPath() string { return c.dbPath }

func (c *catalog) loadTransforms(derivation string) ([]pf.TransformSpec, error) {
	var transforms []pf.TransformSpec

	var rows, err = c.db.Query(`
	SELECT
		transform_id,
		source_name,
		derivation_name,
		source_partitions_json,
		shuffle_key_json,
		shuffle_broadcast,
		read_delay_seconds,
	FROM transform_details
		WHERE derivation_name = ?`,
		derivation,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read transforms from catalog: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var transform pf.TransformSpec

		// Structure is from view 'transform_source_partitions_json' in catalog.sql
		var partitionsFlat []struct {
			Field, Value string
			Exclude      bool
		}
		if err = rows.Scan(
			&transform.Shuffle.Transform,
			&transform.Source.Name,
			&transform.Derivation.Name,
			scanJSON{&partitionsFlat},
			scanJSON{&transform.Shuffle.ShuffleKeyPtr},
			&transform.Shuffle.ReadDelaySeconds,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tranform from catalog: %w", err)
		}

		transform.Source.Partitions.Include.AddValue(labels.Collection, transform.Source.Name.String())
		for _, f := range partitionsFlat {
			if f.Exclude {
				transform.Source.Partitions.Exclude.AddValue(encodePartitionToLabel(f.Field, f.Value))
			} else {
				transform.Source.Partitions.Include.AddValue(encodePartitionToLabel(f.Field, f.Value))
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
