package main

import (
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
	"unicode"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/capture/driver/airbyte"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/protocols/capture"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v3"
)

type cmdDiscover struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Image       string                `long:"image" required:"true" description:"Docker image of the connector to use"`
	Network     string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Prefix      string                `long:"prefix" default:"acmeCo" description:"Prefix of generated catalog entities. For example, an organization or company name."`
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
	var connectorName = escape(strings.Split(imageParts[len(imageParts)-1], ":")[0])

	configPath, err := filepath.Abs(
		fmt.Sprintf("discover-%s.config.yaml", connectorName))
	if err != nil {
		return fmt.Errorf("building config path: %w", err)
	}
	catalogPath, err := filepath.Abs(
		fmt.Sprintf("discover-%s.flow.yaml", connectorName))
	if err != nil {
		return fmt.Errorf("building output catalog path: %w", err)
	}

	// If the configuration file doesn't exist, write it as a stub.
	if w, err := os.OpenFile(configPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600); err == nil {
		fmt.Printf(`
Creating a connector configuration stub at %s.
Edit and update this file, and then run this command again.
`, configPath)

		if err = writeConfigStub(context.Background(), cmd.Image, cmd.Network, w); err != nil {
			_ = os.Remove(configPath) // Don't leave an empty file behind.
		}
		return err
	} else if !os.IsExist(err) {
		return err
	}

	// Discover bindings and write the output catalog.

	configYaml, configRaw, err := readConfig(configPath)
	if err != nil {
		return err
	}
	discovered, err := discoverBindings(context.Background(), cmd.Image, cmd.Network, configRaw)
	if err != nil {
		return err
	}

	type Collection struct {
		Schema interface{}
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
				Image  string     `yaml:"image"`
				Config *yaml.Node `yaml:"config"`
			} `yaml:"connector"`
		} `yaml:"endpoint"`
		Bindings []Binding `yaml:"bindings"`
	}
	var capture Capture
	var hasEmptyKeys bool

	capture.Endpoint.Spec.Image = cmd.Image
	capture.Endpoint.Spec.Config = configYaml.Content[0]

	for _, b := range discovered.Bindings {
		var target = path.Join(cmd.Prefix, escape(b.RecommendedName))

		var schema, resource interface{}
		if err := json.Unmarshal(b.DocumentSchemaJson, &schema); err != nil {
			return fmt.Errorf("decoding schema of %s: %w", target, err)
		} else if err = json.Unmarshal(b.ResourceSpecJson, &resource); err != nil {
			return fmt.Errorf("decoding resource of %s: %w", target, err)
		}

		collections[target] = Collection{
			Key:    b.KeyPtrs,
			Schema: schema,
		}
		capture.Bindings = append(capture.Bindings, Binding{
			Target:   target,
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

	if err = os.Remove(configPath); err != nil {
		return fmt.Errorf("removing config: %w", err)
	}

	return nil
}

func readConfig(path string) (root *yaml.Node, raw json.RawMessage, err error) {
	var iface interface{}
	root = new(yaml.Node)

	if r, err := os.Open(path); err != nil {
		return nil, nil, fmt.Errorf("opening config: %w", err)
	} else if err = yaml.NewDecoder(r).Decode(root); err != nil {
		return nil, nil, fmt.Errorf("decoding config: %w", err)
	}

	if r, err := os.Open(path); err != nil {
		return nil, nil, fmt.Errorf("opening config: %w", err)
	} else if err = yaml.NewDecoder(r).Decode(&iface); err != nil {
		return nil, nil, fmt.Errorf("decoding config: %w", err)
	}

	if raw, err = json.Marshal(iface); err != nil {
		return nil, nil, fmt.Errorf("encoding JSON config: %w", err)
	}

	return root, raw, nil
}

func writeConfigStub(ctx context.Context, image string, connectorNetwork string, w io.WriteCloser) error {
	spec, err := json.Marshal(airbyte.EndpointSpec{
		Image:  image,
		Config: nil,
	})
	if err != nil {
		return fmt.Errorf("encoding spec: %w", err)
	}

	client, err := capture.NewDriver(ctx, pf.EndpointType_AIRBYTE_SOURCE, spec, connectorNetwork, ops.StdLogger())
	if err != nil {
		return fmt.Errorf("building client: %w", err)
	}

	specResponse, err := client.Spec(ctx,
		&pc.SpecRequest{
			EndpointType:     pf.EndpointType_AIRBYTE_SOURCE,
			EndpointSpecJson: spec,
		})
	if err != nil {
		return fmt.Errorf("fetching connector spec: %w", err)
	}

	tmpdir, err := ioutil.TempDir("", "flow-discover")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	var tmpfile = filepath.Join(tmpdir, "schema.yaml")
	mbp.Must(ioutil.WriteFile(tmpfile, specResponse.EndpointSpecSchemaJson, 0600), "writing spec")

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
		Context:             ctx,
		BuildAPI_Config:     buildConfig,
		FileRoot:            "/",
		CaptureDriverFn:     nil, // Not used.
		MaterializeDriverFn: nil, // Not used.
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

			var nn = new(yaml.Node)
			nn.Style = yaml.FlowStyle // renders values inline with the keys instead of on the next line
			nn.Kind = yaml.ScalarNode // seems required to get numbers and bools to render correctly

			nn.FootComment =
				fmt.Sprintf("%s\n%s", loc.Spec.Description, loc.Spec.Types)

			if loc.Spec.MustExist {
				nn.FootComment += " (required)"
			}
			// The explicit tags are necessary for the encoder to know how to render these. They
			// will not be included in the final output.
			switch getDefaultType(&loc.Spec) {
			case pf.JsonTypeString:
				nn.SetString("")
			case pf.JsonTypeInteger:
				nn.Value = "0"
				nn.Tag = "!!int"
			case pf.JsonTypeNumber:
				nn.Value = "0.0"
				nn.Tag = "!!float"
			case pf.JsonTypeBoolean:
				nn.Value = "false"
				nn.Tag = "!!bool"
			case pf.JsonTypeObject:
				nn.Value = "{}"
				nn.Tag = "!!map"
			case pf.JsonTypeArray:
				nn.Value = "[]"
				nn.Tag = "!!seq"
			case pf.JsonTypeNull:
				nn.Tag = "!!null"
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

func discoverBindings(ctx context.Context, image string, connectorNetwork string, config json.RawMessage) (*pc.DiscoverResponse, error) {
	spec, err := json.Marshal(airbyte.EndpointSpec{
		Image:  image,
		Config: config,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding spec: %w", err)
	}

	client, err := capture.NewDriver(ctx, pf.EndpointType_AIRBYTE_SOURCE, spec, connectorNetwork, ops.StdLogger())
	if err != nil {
		return nil, fmt.Errorf("building client: %w", err)
	}

	discovered, err := client.Discover(ctx,
		&pc.DiscoverRequest{
			EndpointType:     pf.EndpointType_AIRBYTE_SOURCE,
			EndpointSpecJson: spec,
		})
	if err != nil {
		return nil, fmt.Errorf("fetching connector bindings: %w", err)
	}

	return discovered, nil
}

func escape(s string) string {
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '-' || r == '_' || r == '/' {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
