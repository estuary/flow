package bindings

import (
	"database/sql"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
)

// BuildError is a user error, encountered during catalog builds.
type BuildError struct {
	// Scope is the resource URL and JSON fragment pointer at which the error occurred.
	Scope string
	// Error is a user-facing description of the build error.
	Error string
}

// BuiltCatalog holds build outputs of the Flow catalog build process.
type BuiltCatalog struct {
	Config pf.BuildAPI_Config
	ID     uuid.UUID
	Errors []BuildError

	Captures         []pf.CaptureSpec
	Collections      []pf.CollectionSpec
	Derivations      []pf.DerivationSpec
	JournalRules     pf.JournalRules
	Locations        []SchemaLocation
	Materializations []pf.MaterializationSpec
	NPMPackage       []byte
	Schemas          pf.SchemaBundle
	ShardRules       pf.ShardRules
	Tests            []pf.TestSpec
}

func loadBuiltCatalog(config pf.BuildAPI_Config) (*BuiltCatalog, error) {
	var id, err = uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("generating build ID: %w", err)
	}

	db, err := sql.Open("sqlite3", config.CatalogPath)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite DB: %w", err)
	}
	defer db.Close()

	var out = &BuiltCatalog{
		Config:  config,
		ID:      id,
		Schemas: pf.SchemaBundle{Bundle: make(map[string]string)},
	}

	if err := loadRows(db,
		`SELECT scope, error FROM errors;`,
		func() []interface{} { return []interface{}{new(string), new(string)} },
		func(l []interface{}) {
			out.Errors = append(out.Errors, BuildError{
				Scope: *l[0].(*string),
				Error: *l[1].(*string),
			})
		},
	); err != nil {
		return nil, fmt.Errorf("loading build errors: %w", err)
	}

	if len(out.Errors) != 0 {
		return out, nil
	}

	if err := loadSpecs(db,
		`SELECT spec FROM built_captures ORDER BY capture ASC;`,
		func() loadableSpec { return new(pf.CaptureSpec) },
		func(l loadableSpec) { out.Captures = append(out.Captures, *l.(*pf.CaptureSpec)) },
	); err != nil {
		return nil, fmt.Errorf("loading captures: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM built_collections ORDER BY collection ASC;`,
		func() loadableSpec { return new(pf.CollectionSpec) },
		func(l loadableSpec) { out.Collections = append(out.Collections, *l.(*pf.CollectionSpec)) },
	); err != nil {
		return nil, fmt.Errorf("loading collections: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM built_derivations ORDER BY derivation ASC;`,
		func() loadableSpec { return new(pf.DerivationSpec) },
		func(l loadableSpec) { out.Derivations = append(out.Derivations, *l.(*pf.DerivationSpec)) },
	); err != nil {
		return nil, fmt.Errorf("loading derivations: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM journal_rules ORDER BY rule ASC;`,
		func() loadableSpec { return new(pf.JournalRules_Rule) },
		func(l loadableSpec) {
			out.JournalRules.Rules = append(out.JournalRules.Rules, *l.(*pf.JournalRules_Rule))
		},
	); err != nil {
		return nil, fmt.Errorf("loading journal rules: %w", err)
	}

	if err := loadRows(db,
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
			out.Locations = append(out.Locations, loc)
		},
	); err != nil {
		return nil, fmt.Errorf("loading schema locations: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM built_materializations ORDER BY materialization ASC;`,
		func() loadableSpec { return new(pf.MaterializationSpec) },
		func(l loadableSpec) {
			out.Materializations = append(out.Materializations, *l.(*pf.MaterializationSpec))
		},
	); err != nil {
		return nil, fmt.Errorf("loading materializations: %w", err)
	}

	if err := loadRows(db,
		`SELECT content FROM resources WHERE content_type = '"NpmPackage"';`,
		func() []interface{} { return []interface{}{&out.NPMPackage} },
		func(_ []interface{}) {},
	); err != nil {
		return nil, fmt.Errorf("loading NPM package: %w", err)
	}

	if err := loadRows(db,
		`SELECT schema, dom FROM schema_docs;`,
		func() []interface{} { return []interface{}{new(string), new(string)} },
		func(l []interface{}) { out.Schemas.Bundle[*l[0].(*string)] = *l[1].(*string) },
	); err != nil {
		return nil, fmt.Errorf("loading schema documents: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM shard_rules ORDER BY rule ASC;`,
		func() loadableSpec { return new(pf.ShardRules_Rule) },
		func(l loadableSpec) { out.ShardRules.Rules = append(out.ShardRules.Rules, *l.(*pf.ShardRules_Rule)) },
	); err != nil {
		return nil, fmt.Errorf("loading shard rules: %w", err)
	}

	if err := loadSpecs(db,
		`SELECT spec FROM built_tests ORDER BY test ASC;`,
		func() loadableSpec { return new(pf.TestSpec) },
		func(l loadableSpec) {
			out.Tests = append(out.Tests, *l.(*pf.TestSpec))
		},
	); err != nil {
		return nil, fmt.Errorf("loading test cases: %w", err)
	}

	return out, nil
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
		} else if next.Unmarshal(b); err != nil {
			return fmt.Errorf("unmarshal spec: %w", err)
		} else if err = next.Validate(); err != nil {
			return fmt.Errorf("validating spec %s: %w", next.String(), err)
		}
		loadedFn(next)
	}
	return rows.Err()
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
