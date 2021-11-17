package catalog

import (
	"database/sql"
	"fmt"

	pf "github.com/estuary/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
)

// Extract is a convenience for testing. It opens a given catalog database,
// ensures it built without errors, invokes the provided callback,
// and then closes the database.
func Extract(path string, fn func(db *sql.DB) error) error {
	return extract(path, func(db *sql.DB) error {
		// Sanity-check that no build errors occurred.
		if errors, err := LoadAllErrors(db); err != nil {
			return fmt.Errorf("loading catalog errors: %w", err)
		} else if len(errors) != 0 {
			return fmt.Errorf("catalog has %d errors: %v", len(errors), errors)
		}

		return fn(db)
	})
}

func extract(path string, fn func(db *sql.DB) error) error {
	var db, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", path))
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}
	defer db.Close()

	return fn(db)
}

// LoadBuildConfig loads the Config under which this catalog was built.
func LoadBuildConfig(db *sql.DB) (pf.BuildAPI_Config, error) {
	var out pf.BuildAPI_Config
	return out, loadOneSpec(db, `SELECT build_config FROM meta`, &out)
}

// Error is a recorded error encountered during the catalog build.
type Error struct {
	// Scope is the resource URL and JSON fragment pointer at which the error occurred.
	Scope string
	// Error is a user-facing description of the build error.
	Error string
}

// LoadAllErrors loads all errors.
func LoadAllErrors(db *sql.DB) ([]Error, error) {
	var out []Error
	var err = loadRows(db,
		`SELECT scope, error FROM errors;`,
		func() []interface{} { return []interface{}{new(string), new(string)} },
		func(l []interface{}) {
			out = append(out, Error{
				Scope: *l[0].(*string),
				Error: *l[1].(*string),
			})
		},
	)
	return out, err
}

// LoadAllCollections loads all collections.
func LoadAllCollections(db *sql.DB) ([]*pf.CollectionSpec, error) {
	var out []*pf.CollectionSpec
	var err = loadSpecs(db,
		`SELECT spec FROM built_collections ORDER BY collection ASC;`,
		func() loadableSpec { return new(pf.CollectionSpec) },
		func(l loadableSpec) { out = append(out, l.(*pf.CollectionSpec)) },
	)
	return out, err
}

// LoadCollection by its name.
func LoadCollection(db *sql.DB, name string) (*pf.CollectionSpec, error) {
	var out = new(pf.CollectionSpec)
	return out, loadOneSpec(db, `SELECT spec FROM built_collections WHERE collection = ?;`, out, name)
}

// LoadAllCaptures loads all captures.
func LoadAllCaptures(db *sql.DB) ([]*pf.CaptureSpec, error) {
	var out []*pf.CaptureSpec
	var err = loadSpecs(db,
		`SELECT spec FROM built_captures ORDER BY capture ASC;`,
		func() loadableSpec { return new(pf.CaptureSpec) },
		func(l loadableSpec) { out = append(out, l.(*pf.CaptureSpec)) },
	)
	return out, err
}

// LoadCapture by its name.
func LoadCapture(db *sql.DB, name string) (*pf.CaptureSpec, error) {
	var out = new(pf.CaptureSpec)
	return out, loadOneSpec(db, `SELECT spec FROM built_captures WHERE capture = ?;`, out, name)
}

// LoadAllDerivations loads all derivations.
func LoadAllDerivations(db *sql.DB) ([]*pf.DerivationSpec, error) {
	var out []*pf.DerivationSpec
	var err = loadSpecs(db,
		`SELECT spec FROM built_derivations ORDER BY derivation ASC;`,
		func() loadableSpec { return new(pf.DerivationSpec) },
		func(l loadableSpec) { out = append(out, l.(*pf.DerivationSpec)) },
	)
	return out, err
}

// LoadDerivation by its name.
func LoadDerivation(db *sql.DB, name string) (*pf.DerivationSpec, error) {
	var out = new(pf.DerivationSpec)
	return out, loadOneSpec(db, `SELECT spec FROM built_derivations WHERE derivation = ?;`, out, name)
}

// LoadAllMaterializations loads all materializations.
func LoadAllMaterializations(db *sql.DB) ([]*pf.MaterializationSpec, error) {
	var out []*pf.MaterializationSpec
	var err = loadSpecs(db,
		`SELECT spec FROM built_materializations ORDER BY materialization ASC;`,
		func() loadableSpec { return new(pf.MaterializationSpec) },
		func(l loadableSpec) { out = append(out, l.(*pf.MaterializationSpec)) },
	)
	return out, err
}

// LoadMaterialization by its name.
func LoadMaterialization(db *sql.DB, name string) (*pf.MaterializationSpec, error) {
	var out = new(pf.MaterializationSpec)
	return out, loadOneSpec(db, `SELECT spec FROM built_materializations WHERE materialization = ?;`, out, name)
}

// LoadAllTests loads all tests.
func LoadAllTests(db *sql.DB) ([]*pf.TestSpec, error) {
	var out []*pf.TestSpec
	var err = loadSpecs(db,
		`SELECT spec FROM built_tests ORDER BY test ASC;`,
		func() loadableSpec { return new(pf.TestSpec) },
		func(l loadableSpec) {
			out = append(out, l.(*pf.TestSpec))
		},
	)
	return out, err
}

// LoadNPMPackage loads the NPM package of the catalog.
func LoadNPMPackage(db *sql.DB) ([]byte, error) {
	var out []byte
	var err = db.QueryRow(
		`SELECT content FROM resources WHERE content_type = '"NPM_PACKAGE"';`,
	).Scan(&out)

	if err != nil {
		return nil, fmt.Errorf("loading NPM package: %w", err)
	}
	return out, nil
}

// SchemaLocation is static inference of a location within a schema document.
type SchemaLocation struct {
	// URL of the schema which is inferred, inclusive of any fragment pointer.
	Schema string
	// A location within a document verified by this schema,
	// relative to the schema root.
	Location string
	// Inference at this schema location.
	Spec pf.Inference
}

// LoadAllInferences loads all inferences.
func LoadAllInferences(db *sql.DB) ([]SchemaLocation, error) {
	var out []SchemaLocation
	var err = loadRows(db,
		`SELECT schema, location, spec FROM inferences ORDER BY schema, location ASC;`,
		func() []interface{} { return []interface{}{new(string), new(string), new([]byte)} },
		func(l []interface{}) {
			var loc = SchemaLocation{
				Schema:   *l[0].(*string),
				Location: *l[1].(*string),
			}
			if err := loc.Spec.Unmarshal(*l[2].(*[]byte)); err != nil {
				panic(err) // TODO plumb this better.
			}
			out = append(out, loc)
		},
	)
	return out, err
}

// LoadSchemaBundle loads the bundle of schema documents.
func LoadSchemaBundle(db *sql.DB) (pf.SchemaBundle, error) {
	var out = pf.SchemaBundle{
		Bundle: make(map[string]string),
	}
	var err = loadRows(db,
		`SELECT schema, dom FROM schema_docs;`,
		func() []interface{} { return []interface{}{new(string), new(string)} },
		func(l []interface{}) { out.Bundle[*l[0].(*string)] = *l[1].(*string) },
	)
	return out, err
}

type loadableSpec interface {
	Unmarshal([]byte) error
	Validate() error
	String() string
}

func loadSpecs(
	db *sql.DB,
	query string,
	newFn func() loadableSpec,
	loadedFn func(loadableSpec),
) error {
	var rows, err = db.Query(query)
	if err != nil {
		return fmt.Errorf("query(%q): %w", query, err)
	}
	defer rows.Close()

	var b []byte
	for rows.Next() {
		var next = newFn()

		if err := rows.Scan(&b); err != nil {
			return fmt.Errorf("scanning collection: %w", err)
		} else if err = next.Unmarshal(b); err != nil {
			return fmt.Errorf("unmarshal spec: %w", err)
		} else if err = next.Validate(); err != nil {
			return fmt.Errorf("validating spec %s: %w", next.String(), err)
		}
		loadedFn(next)
	}
	return rows.Err()
}

func loadOneSpec(
	db *sql.DB,
	query string,
	spec loadableSpec,
	args ...interface{},
) error {
	var row = db.QueryRow(query, args...)

	var b []byte
	if err := row.Scan(&b); err != nil {
		if err != sql.ErrNoRows {
			err = fmt.Errorf("query(%q): %w", query, err)
		}
		return err
	} else if err = spec.Unmarshal(b); err != nil {
		return fmt.Errorf("unmarshal spec: %w", err)
	} else if err = spec.Validate(); err != nil {
		return fmt.Errorf("validating spec %s: %w", spec.String(), err)
	}
	return nil
}

func loadRows(
	db *sql.DB,
	query string,
	newFn func() []interface{},
	loadedFn func([]interface{}),
) error {
	var rows, err = db.Query(query)
	if err != nil {
		return fmt.Errorf("query(%q): %w", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		var next = newFn()

		if err := rows.Scan(next...); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		loadedFn(next)
	}
	return rows.Err()
}
