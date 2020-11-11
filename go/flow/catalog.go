package flow

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	pf "github.com/estuary/flow/go/protocol"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	pb "go.gazette.dev/core/broker/protocol"
)

// Catalog is a Catalog database which has been fetched to a local file.
type Catalog struct {
	dbPath string
	db     *sql.DB
}

// NewCatalog copies the catalog DB at the given |url| to the local |tempDir|, and opens it.
func NewCatalog(pathOrURL, tempDir string) (*Catalog, error) {
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	var path = pathOrURL
	if url, err := url.Parse(pathOrURL); err == nil && url.Scheme != "" {
		if path, err = fetchRemote(url, tempDir); err != nil {
			return nil, fmt.Errorf("fetching remote catalog: %w", err)
		}
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?immutable=true&mode=ro")
	if err != nil {
		return nil, fmt.Errorf("opening catalog database %v: %w", path, err)
	}

	return &Catalog{
		dbPath: path,
		db:     db,
	}, nil
}

func fetchRemote(url *url.URL, tempDir string) (string, error) {
	// Download catalog from URL to a local file.
	var dbFile, err = ioutil.TempFile(tempDir, "catalog-db")
	if err != nil {
		return "", fmt.Errorf("failed to create local DB tempfile: %w", err)
	}

	if resp, err := http.Get(url.String()); err != nil {
		return "", fmt.Errorf("failed to request catalog URL: %w", err)
	} else if _, err = io.Copy(dbFile, resp.Body); err != nil {
		return "", fmt.Errorf("failed to copy catalog to local file: %w", err)
	} else if err = resp.Body.Close(); err != nil {
		return "", fmt.Errorf("failed to close catalog response: %w", err)
	} else if err = dbFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close local catalog file: %w", err)
	}
	return dbFile.Name(), nil
}

// LocalPath returns the local path of the catalog.
func (catalog *Catalog) LocalPath() string { return catalog.dbPath }

// LoadDerivedCollection loads the named derived collection from the catalog.
func (catalog *Catalog) LoadDerivedCollection(derivation string) (pf.CollectionSpec, error) {
	var collections, err = scanCollections(catalog.db.Query(
		selectCollection+"WHERE is_derivation AND collection_name = ?", derivation))

	if err != nil {
		return pf.CollectionSpec{}, err
	} else if len(collections) != 1 {
		return pf.CollectionSpec{}, fmt.Errorf("no such derived collection %q", derivation)
	}
	return collections[0], nil
}

// Close the Catalog database, rendering it unusable.
func (catalog *Catalog) Close() error {
	return catalog.db.Close()
}

// LoadCollection loads the collection with the given name from the catalog, or returns an error if
// one is not found
func (catalog *Catalog) LoadCollection(name string) (pf.CollectionSpec, error) {
	var collections, err = scanCollections(catalog.db.Query(
		selectCollection+"WHERE collection_name = ?", name))

	if err != nil {
		return pf.CollectionSpec{}, err
	} else if len(collections) != 1 {
		return pf.CollectionSpec{}, fmt.Errorf("no such collection %q", name)
	}
	return collections[0], nil
}

// LoadCapturedCollections loads all captured collections from the catalog.
func (catalog *Catalog) LoadCapturedCollections() (map[pf.Collection]*pf.CollectionSpec, error) {
	var specs, err = scanCollections(catalog.db.Query(selectCollection + "WHERE NOT is_derivation"))
	if err != nil {
		return nil, err
	}

	var out = make(map[pf.Collection]*pf.CollectionSpec)
	for i := range specs {
		out[specs[i].Name] = &specs[i]
	}
	return out, nil
}

// LoadMaterializationTarget load the target with the given name from the catalog, or returns an
// error if the target is not found
func (catalog *Catalog) LoadMaterializationTarget(targetName string) (*materialize.Materialization, error) {
	stmt, err := catalog.db.Prepare(queryMaterialization)
	if err != nil {
		return nil, err
	}

	materialization := new(materialize.Materialization)
	materialization.TargetName = targetName
	row := stmt.QueryRow(targetName)

	err = row.Scan(
		&materialization.CatalogDBID,
		&materialization.TargetURI,
		&materialization.TargetType,
	)
	if err != nil {
		return nil, err
	}
	return materialization, nil
}

const queryMaterialization = `
    SELECT
        target_id, target_uri, target_type
    FROM
        materialization_targets
    WHERE
        materialization_targets.target_name = ?;
`

const selectCollection = `SELECT spec_json FROM collections_json `

func scanCollections(rows *sql.Rows, err error) ([]pf.CollectionSpec, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to read collections from catalog: %w", err)
	}
	defer rows.Close()

	var collections []pf.CollectionSpec
	for rows.Next() {
		var collection pf.CollectionSpec

		if err = rows.Scan(scanJSON{&collection}); err != nil {
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
				PathPostfixTemplate: `utc_date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/utc_hour={{.Spool.FirstAppendTime.Format "15"}}`,
				FlushInterval:       time.Hour,
			},
		}
		collection.UuidPtr = pf.DocumentUUIDPointer
		collection.AckJsonTemplate = pf.DocumentAckJSONTemplate

		collections = append(collections, collection)
	}
	return collections, nil
}

// LoadTransforms returns []TransformSpecs of all transforms of the given derivation.
func (catalog *Catalog) LoadTransforms(derivation string) ([]pf.TransformSpec, error) {
	var transforms []pf.TransformSpec

	var rows, err = catalog.db.Query(`
	SELECT
		transform_name,
		transform_id,
		source_name,
		derivation_name,
		uses_source_key,
		update_id IS NULL, -- Filter R-Clocks,
		NULL,              -- Hash.
		source_selector_json,
		shuffle_key_json,
		IFNULL(read_delay_seconds, 0)
	FROM transform_details
		WHERE derivation_name = ?`,
		derivation,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read transforms from catalog: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tf pf.TransformSpec

		var selector struct {
			Include map[string][]interface{}
			Exclude map[string][]interface{}
		}
		if err = rows.Scan(
			&tf.Name,
			&tf.CatalogDbId,
			&tf.Source.Name,
			&tf.Derivation.Name,
			&tf.Shuffle.UsesSourceKey,
			&tf.Shuffle.FilterRClocks,
			scanJSON{&tf.Shuffle.Hash},
			scanJSON{&selector},
			scanJSON{&tf.Shuffle.ShuffleKeyPtr},
			&tf.Shuffle.ReadDelaySeconds,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tranform from catalog: %w", err)
		}

		tf.Source.Partitions.Include.AddValue(labels.Collection, tf.Source.Name.String())
		if err = addFieldLabels(&tf.Source.Partitions.Include, selector.Include); err != nil {
			return nil, err
		}
		if err = addFieldLabels(&tf.Source.Partitions.Exclude, selector.Exclude); err != nil {
			return nil, err
		}
		transforms = append(transforms, tf)
	}

	if len(transforms) == 0 {
		return nil, fmt.Errorf("read no transforms for derivation %v", derivation)
	}
	return transforms, nil
}

func addFieldLabels(set *pb.LabelSet, fields map[string][]interface{}) error {
	var arena pf.Arena

	for field, values := range fields {
		for _, value := range values {
			var vv, err = pf.ValueFromInterface(&arena, value)
			if err != nil {
				return fmt.Errorf("building label for field %s value %#v: %w", field, value, err)
			}
			set.AddValue(labels.FieldPrefix+field, string(vv.EncodePartition(nil, arena)))
		}
	}
	return nil
}

type scanJSON struct {
	v interface{}
}

func (j scanJSON) Scan(value interface{}) error {
	switch v := value.(type) {
	case string:
		return json.Unmarshal([]byte(v), j.v)
	case []byte:
		return json.Unmarshal(v, j.v)
	case nil:
		return nil
	default:
		return fmt.Errorf("scanning json: %v is invalid type", value)
	}
}
