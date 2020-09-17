package flow

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
)

// Catalog is a Catalog database which has been fetched to a local file.
type Catalog struct {
	dbPath string
	db     *sql.DB
}

// NewCatalog copies the catalog DB at the given |url| to the local |tempDir|, and opens it.
func NewCatalog(url, tempDir string) (*Catalog, error) {
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

	return &Catalog{
		dbPath: dbPath,
		db:     db,
	}, nil
}

// LocalPath returns the local path of the catalog.
func (c *Catalog) LocalPath() string { return c.dbPath }

// LoadDerivedCollection loads the named derived collection from the catalog.
func (c *Catalog) LoadDerivedCollection(derivation string) (pf.CollectionSpec, error) {
	var collections, err = scanCollections(c.db.Query(
		selectCollection+"WHERE is_derivation AND collection_name = ?", derivation))

	if err != nil {
		return pf.CollectionSpec{}, err
	} else if len(collections) != 1 {
		return pf.CollectionSpec{}, fmt.Errorf("no such derived collection %q", derivation)
	}
	return collections[0], nil
}

// LoadCapturedCollections loads all captured collections from the catalog.
func (c *Catalog) LoadCapturedCollections() ([]pf.CollectionSpec, error) {
	return scanCollections(c.db.Query(selectCollection + "WHERE NOT is_derivation"))
}

const selectCollection = `
	SELECT
		collection_name,
		schema_uri,
		key_json,
		partitions_json,
		projections_json
	FROM collection_details
`

func scanCollections(rows *sql.Rows, err error) ([]pf.CollectionSpec, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to read collections from catalog: %w", err)
	}
	defer rows.Close()

	var collections []pf.CollectionSpec
	for rows.Next() {
		var collection pf.CollectionSpec

		if err = rows.Scan(
			&collection.Name,
			&collection.SchemaUri,
			scanJSON{&collection.KeyPtrs},
			scanJSON{&collection.Partitions},
			scanJSON{&collection.Projections},
		); err != nil {
			return nil, fmt.Errorf("failed to scan collection from catalog: %w", err)
		}

		// TODO(johnny): Draw these from the catalog DB.
		collection.JournalSpec = pb.JournalSpec{
			Replication: 1,
			Fragment: pb.JournalSpec_Fragment{
				Length:              1 << 28, // 256MB.
				Stores:              []pb.FragmentStore{"file:///"},
				CompressionCodec:    pb.CompressionCodec_SNAPPY,
				RefreshInterval:     5 * time.Minute,
				PathPostfixTemplate: `date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/hour={{.Spool.FirstAppendTime.Format "15"}}`,
				FlushInterval:       time.Hour,
			},
		}
		collection.UuidPtr = pf.DocumentUUIDPointer
		collection.AckJsonTemplate = pf.DocumentAckJSONTemplate
	}
	return collections, nil
}

// LoadTransforms returns []TransformSpecs of all transforms of the given derivation.
func (c *Catalog) LoadTransforms(derivation string) ([]pf.TransformSpec, error) {
	var transforms []pf.TransformSpec

	var rows, err = c.db.Query(`
	SELECT
		transform_name,
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
			&transform.Name,
			&transform.CatalogDbId,
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
