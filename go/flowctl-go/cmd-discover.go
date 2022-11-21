package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v3"
)

type cmdDiscover struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Image       string                `long:"image" required:"true" description:"Docker image of the connector to use"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Prefix      string                `long:"prefix" default:"acmeCo" description:"Prefix of generated catalog entities. For example, an organization or company name."`
	Directory   string                `long:"directory" description:"Output directory for catalog source files. Defaults to --prefix"`
}

func (cmd cmdDiscover) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var imageParts = strings.Split(cmd.Image, "/")
	var connectorName = strings.Split(imageParts[len(imageParts)-1], ":")[0]

	// Directory defaults to --prefix.
	if cmd.Directory == "" {
		cmd.Directory = cmd.Prefix
	}

	if err := os.MkdirAll(cmd.Directory, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	} else if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("getting absolute directory: %w", err)
	}

	var configName = fmt.Sprintf("%s.config.yaml", connectorName)
	var configPath = filepath.Join(cmd.Directory, configName)
	var catalogPath = filepath.Join(cmd.Directory, fmt.Sprintf("%s.flow.yaml", connectorName))

	// If the configuration file doesn't exist, write it as a stub.
	if w, err := os.OpenFile(configPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600); err == nil {
		fmt.Printf(`
Creating a connector configuration stub at %s.
Edit and update this file, and then run this command again.
`, configPath)

		if err = cmd.writeConfigStub(context.Background(), w); err != nil {
			_ = os.Remove(configPath) // Don't leave an empty file behind.
		}
		return err
	} else if !os.IsExist(err) {
		return err
	}

	// Discover bindings and write the output catalog.
	discovered, err := apiDiscover{
		Log:         cmd.Log,
		Diagnostics: cmd.Diagnostics,
		Image:       cmd.Image,
		Network:     cmd.Network,
		Config:      configPath,
		Output:      "", // Not required.
	}.execute(context.Background())
	if err != nil {
		return err
	} else if err := discovered.Validate(); err != nil {
		return err
	}

	type Collection struct {
		Schema string
		Key    []string `yaml:",flow"`
	}
	var collections = make(map[string]Collection)

	type Binding struct {
		Resource interface{} `yaml:"resource"`
		Target   string      `yaml:"target"`
	}
	type Capture struct {
		Endpoint struct {
			Spec struct {
				Image  string `yaml:"image"`
				Config string `yaml:"config"`
			} `yaml:"connector"`
		} `yaml:"endpoint"`
		Bindings []Binding `yaml:"bindings"`
	}
	var capture Capture
	var hasEmptyKeys bool

	capture.Endpoint.Spec.Image = cmd.Image
	capture.Endpoint.Spec.Config = configName

	for _, b := range discovered.Bindings {
		var collection = path.Join(cmd.Prefix, b.RecommendedName.String())
		var schemaName = fmt.Sprintf("%s.schema.yaml", b.RecommendedName)
		var outputPath = filepath.Join(cmd.Directory, schemaName)
		var outputDir = path.Dir(outputPath)

		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("creating output directory: %w", err)
		}

		var schema, resource interface{}
		if err := json.Unmarshal(b.DocumentSchemaJson, &schema); err != nil {
			return fmt.Errorf("decoding schema of %s: %w", collection, err)
		} else if err = json.Unmarshal(b.ResourceSpecJson, &resource); err != nil {
			return fmt.Errorf("decoding resource of %s: %w", collection, err)
		}

		// Write out schema file.
		var schemaBytes bytes.Buffer
		var enc = yaml.NewEncoder(&schemaBytes)
		enc.SetIndent(2)
		if err := enc.Encode(schema); err != nil {
			return fmt.Errorf("encoding schema: %w", err)
		} else if err = enc.Close(); err != nil {
			return fmt.Errorf("encoding schema: %w", err)
		} else if err = ioutil.WriteFile(filepath.Join(cmd.Directory, schemaName), schemaBytes.Bytes(), 0644); err != nil {
			return fmt.Errorf("writing schema: %w", err)
		}

		collections[collection] = Collection{
			Key:    b.KeyPtrs,
			Schema: schemaName,
		}
		capture.Bindings = append(capture.Bindings, Binding{
			Target:   collection,
			Resource: resource,
		})

		if len(b.KeyPtrs) == 0 {
			hasEmptyKeys = true
		}
	}

	w, err := os.Create(catalogPath)
	if err != nil {
		return fmt.Errorf("opening output catalog: %w", err)
	}
	var enc = yaml.NewEncoder(w)
	enc.SetIndent(2)

	if err = enc.Encode(struct {
		Collections map[string]Collection
		Captures    map[string]Capture
	}{
		collections,
		map[string]Capture{
			path.Join(cmd.Prefix, connectorName): capture,
		},
	}); err == nil {
		err = enc.Close()
	}
	if err == nil {
		err = w.Close()
	}

	if err != nil {
		return fmt.Errorf("writing output catalog: %w", err)
	}

	fmt.Printf(`
Created a Flow catalog at %s
with discovered collections and capture bindings.
`, catalogPath)

	if hasEmptyKeys {
		fmt.Print(`
A native key couldn't be determined for all collections.
You must manually add appropriate keys, and update associated collection schemas
(for example, by marking corresponding properties as "required").
`)
	}

	return nil
}

func (cmd cmdDiscover) writeConfigStub(ctx context.Context, w io.WriteCloser) error {
	var spec = apiSpec{
		Log:         cmd.Log,
		Diagnostics: cmd.Diagnostics,
		Image:       cmd.Image,
		Network:     cmd.Network,
	}

	var resp, err = spec.execute(ctx)
	if err != nil {
		return fmt.Errorf("querying connector spec: %w", err)
	}

	// TODO(johnny): Factor out into a schema tool.
	tmpdir, err := ioutil.TempDir("", "flow-discover")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	var tmpfile = filepath.Join(tmpdir, "schema.yaml")
	mbp.Must(ioutil.WriteFile(tmpfile, resp.EndpointSpecSchema, 0600), "writing spec")

	// Build the schema
	var buildConfig = pf.BuildAPI_Config{
		BuildId:    newBuildID(),
		Directory:  tmpdir,
		Source:     tmpfile,
		SourceType: pf.ContentType_JSON_SCHEMA,
	}
	// Cleanup output database.
	defer func() { _ = os.Remove(buildConfig.OutputPath()) }()

	if err = bindings.BuildCatalog(bindings.BuildArgs{
		Context:         ctx,
		BuildAPI_Config: buildConfig,
		FileRoot:        "/",
	}); err != nil {
		return fmt.Errorf("building schema catalog: %w", err)
	}

	// Load extracted schema locations.
	var locations []catalog.SchemaLocation
	if err = catalog.Extract(buildConfig.OutputPath(), func(db *sql.DB) error {
		if locations, err = catalog.LoadAllInferences(db); err != nil {
			return fmt.Errorf("loading inferences: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	var config interface{}

	// Visit leaf-most schema locations first.
	// Because we're creating yaml.Nodes instead of []interface{}
	// or map[string]interface{}, ptr.Create() is unable to create
	// a sub-location after visiting its parent.
	sort.Slice(locations, func(i int, j int) bool {
		return len(locations[i].Location) > len(locations[j].Location)
	})

	for _, loc := range locations {
		if ptr, err := flow.NewPointer(loc.Location); err != nil {
			return fmt.Errorf("build pointer: %w", err)
		} else if node, err := ptr.Create(&config); err != nil {
			return fmt.Errorf("creating location %q: %w", loc.Location, err)
		} else if *node == nil {
			var nn, err = buildStubNode(&loc.Spec)
			if err != nil {
				return fmt.Errorf("location %s: %w", loc.Location, err)
			}
			*node = nn
		}
	}

	var enc = yaml.NewEncoder(w)
	enc.SetIndent(2)

	if err = enc.Encode(config); err == nil {
		err = w.Close()
	}
	if err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	return nil
}

// getDefaultType returns the type to use for generating a default value for endpoint configuration.
// It will always prefer a scalar type if a location allows multiple types.
func getDefaultType(inference *pf.Inference) string {
	var fallback string
	for _, ty := range inference.Types {
		if ty == pf.JsonTypeString || ty == pf.JsonTypeBoolean || ty == pf.JsonTypeInteger || ty == pf.JsonTypeNumber {
			return ty
		} else {
			fallback = ty
		}
	}
	return fallback
}

func buildStubNode(inference *pf.Inference) (*yaml.Node, error) {
	var node = new(yaml.Node)

	if len(inference.DefaultJson) != 0 {
		if err := yaml.NewDecoder(bytes.NewReader(inference.DefaultJson)).Decode(node); err != nil {
			return nil, fmt.Errorf("decoding schema `default` value: %w", err)
		}
		node = node.Content[0] // Unwrap root document node.
	} else {
		// The explicit tags are necessary for the encoder to know how to render these. They
		// will not be included in the final output.
		switch getDefaultType(inference) {
		case pf.JsonTypeString:
			node.SetString("")
		case pf.JsonTypeInteger:
			node.Value = "0"
			node.Tag = "!!int"
		case pf.JsonTypeNumber:
			node.Value = "0.0"
			node.Tag = "!!float"
		case pf.JsonTypeBoolean:
			node.Value = "false"
			node.Tag = "!!bool"
		case pf.JsonTypeObject:
			node.Value = "{}"
			node.Tag = "!!map"
		case pf.JsonTypeArray:
			node.Value = "[]"
			node.Tag = "!!seq"
		case pf.JsonTypeNull:
			node.Tag = "!!null"
		}

		// Required to get numbers and booleans to render correctly (?).
		node.Kind = yaml.ScalarNode
	}

	// Renders values inline with keys, instead of on the next line.
	node.Style = yaml.FlowStyle

	node.FootComment =
		fmt.Sprintf("%s\n%s", inference.Description, inference.Types)

	if inference.Exists == pf.Inference_MUST {
		node.FootComment += " (required)"
	}
	if inference.Secret {
		node.FootComment += " (secret)"
	}

	return node, nil
}
