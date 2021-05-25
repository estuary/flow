package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v3"
)

type cmdDiscover struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

var connectors = map[string]string{
	"GitHub":        "airbyte/source-github-singer:latest",
	"Google Sheets": "airbyte/source-google-sheets:latest",
}

type state struct {
	app *tview.Application

	ctx     context.Context
	cancel  context.CancelFunc
	catalog *ConnectorCatalog
	image   string
	spec    json.RawMessage
	config  interface{}
	streams map[string]struct{}
	err     error
}

func (s *state) addSelectConnectorPage() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var connectorKeys []string
	for k := range connectors {
		connectorKeys = append(connectorKeys, k)
	}
	sort.Strings(connectorKeys)

	var form = tview.NewForm()
	form.
		AddDropDown("connector", connectorKeys, -1, func(option string, ind int) {
			if ind == -1 {
				// No-op
			} else if v := form.GetFormItemByLabel("image"); v != nil {
				v.(*tview.InputField).SetText(connectors[option])
			}
		}).
		AddInputField("image", "", 60, nil, func(text string) { s.image = text }).
		AddButton("Next", s.addGetConnectorSpecPage).
		AddButton("Quit", func() { s.app.Stop() }).
		SetBorder(true).
		SetTitle(" Select a connector: ").
		SetTitleAlign(tview.AlignLeft)

	s.app.SetRoot(form, true)
}

func (s *state) addGetConnectorSpecPage() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var modal = tview.
		NewModal().
		SetText(" Fetching connector spec... ")

	s.app.SetRoot(modal, true)

	go func() {
		s.spec, s.err = getConnectorSpec(s.image)

		s.app.QueueUpdateDraw(func() {
			s.addConnectorConfigPage()
		})
	}()
}

func (s *state) addConnectorConfigPage() {
	s.config = nil

	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var built *bindings.BuiltCatalog
	if built, s.err = inferFromJSONSchema(s.spec); s.err != nil {
		s.app.Stop()
		return
	}

	var form = tview.NewForm()
	var desc = tview.NewTextView()
	var descTexts []string

	desc.
		SetBorder(true).
		SetTitle("Description")

	// Loop which queries for the current focus item and updates the description.
	go func() {
		var ticker = time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}

			s.app.QueueUpdateDraw(func() {
				var ind, _ = form.GetFocusedItemIndex()
				if ind != -1 {
					desc.SetText(descTexts[ind])
				}
			})
		}
	}()

	for _, loc := range built.Locations {
		var ptr, err = flow.NewPointer(loc.Location)
		if err != nil {
			panic(err)
		} else if ptr.IsEmpty() {
			continue // Ignore document root.
		}

		var label = loc.Spec.Title
		if label == "" {
			label = loc.Location[1:]
		}

		if loc.Spec.MustExist {
			label = "* " + label
		} else {
			label = "  " + label
		}
		descTexts = append(descTexts,
			fmt.Sprintf("%s\n\n%s", loc.Spec.Description, loc.Spec.Types))

		form.AddInputField(label, "", 80, nil, func(text string) {
			if loc, err := ptr.Create(&s.config); err != nil {
				s.err = err
				s.app.Stop()
			} else {
				*loc = text
			}
		})
	}

	form.AddButton("Next", s.addGetConnectorCatalog)
	form.AddButton("Back", s.addSelectConnectorPage)
	form.AddButton("Quit", func() { s.app.Stop() })

	var grid = tview.NewGrid()
	grid.
		SetRows(-4, -1).
		SetBorder(true).
		SetTitle("Configure the connector").
		SetTitleAlign(tview.AlignLeft)

	grid.AddItem(form, 0, 0, 1, 1, 0, 0, true)
	grid.AddItem(desc, 1, 0, 1, 1, 0, 0, false)

	s.app.SetRoot(grid, true)
}

func (s *state) addGetConnectorCatalog() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var modal = tview.
		NewModal().
		SetText(" Fetching connector catalog... ")

	s.app.SetRoot(modal, true)

	go func() {
		s.catalog, s.err = getConnectorCatalog(s.image, s.config)

		s.app.QueueUpdateDraw(func() {
			if s.err != nil {
				s.app.Stop()
				return
			}
			s.addConnectorCatalogPage()
		})
	}()
}

func extendNavigation(event *tcell.EventKey) *tcell.EventKey {
	switch event.Key() {
	/*
		case tcell.KeyUp, tcell.KeyLeft:
			return tcell.NewEventKey(tcell.KeyBacktab, ' ', tcell.ModNone)
		case tcell.KeyDown, tcell.KeyRight:
			return tcell.NewEventKey(tcell.KeyTab, ' ', tcell.ModNone)
	*/
	default:
		return event
	}
}

func (s *state) addConnectorCatalogPage() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var form = tview.NewForm()
	var desc = tview.NewTextView()
	var descTexts []string

	desc.
		SetBorder(true).
		SetTitle("Description")

	// Loop which queries for the current focus item and updates the description.
	go func() {
		var ticker = time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}

			s.app.QueueUpdateDraw(func() {
				var ind, _ = form.GetFocusedItemIndex()
				if ind != -1 {
					desc.SetText(descTexts[ind])
				}
			})
		}
	}()

	for _, str := range s.catalog.Streams {
		var name = str.Name

		var cb = tview.NewCheckbox()
		cb.SetLabel(name)
		cb.SetChangedFunc(func(checked bool) {
			if checked {
				s.streams[name] = struct{}{}
			} else {
				delete(s.streams, name)
			}
		})
		cb.SetInputCapture(extendNavigation)
		form.AddFormItem(cb)

		var built *bindings.BuiltCatalog
		if built, s.err = inferFromJSONSchema(str.JSONSchema); s.err != nil {
			s.app.Stop()
			return
		}
		var parts []string

		for _, loc := range built.Locations {
			if loc.Location == "" {
				continue // Ignore root.
			}
			parts = append(parts, fmt.Sprintf("%s: %s", loc.Location[1:], loc.Spec.Types))

			if loc.Spec.Title != "" {
				parts = append(parts, "\t"+loc.Spec.Title)
			}
			if loc.Spec.Description != "" {
				parts = append(parts, "\t"+loc.Spec.Description)
			}
		}
		descTexts = append(descTexts, strings.Join(parts, "\n"))
	}

	form.AddButton("Save & Exit", func() {
		var f, err = os.Create("endpoint.flow.yaml")
		if err != nil {
			s.err = err
			s.app.Stop()
			return
		}
		if s.err = s.write(f); s.err == nil {
			s.err = f.Close()
		}
		s.app.Stop()
	})
	form.AddButton("Back", s.addConnectorConfigPage)
	form.AddButton("Quit", func() { s.app.Stop() })

	for b := 0; b != form.GetButtonCount(); b++ {
		form.GetButton(b).SetInputCapture(extendNavigation)
	}

	var grid = tview.NewGrid()
	grid.
		SetColumns(-1, -4).
		SetBorder(true).
		SetTitle("Select connector streams").
		SetTitleAlign(tview.AlignLeft)

	grid.AddItem(form, 0, 0, 1, 1, 0, 0, true)
	grid.AddItem(desc, 0, 1, 1, 1, 0, 0, false)

	s.app.SetRoot(grid, true)
}

func (s *state) write(w io.Writer) error {
	type Endpoint struct {
		Image  string
		Config interface{}
	}
	type Collection struct {
		Schema interface{}
		Key    []string `yaml:",flow"`
	}
	type Capture struct {
		Target struct {
			Name string
		}
		Endpoint struct {
			Name string

			Config struct {
				Stream string
			}
		}
	}

	var endpoint = "the/endpoint"
	var collections = map[string]Collection{}
	var captures []Capture

	for _, str := range s.catalog.Streams {
		if _, ok := s.streams[str.Name]; !ok {
			continue
		}

		var schema interface{}
		if err := json.Unmarshal(str.JSONSchema, &schema); err != nil {
			return err
		}

		collections[str.Name] = Collection{
			Schema: schema,
			Key:    []string{"/key"},
		}

		var capture Capture
		capture.Target.Name = str.Name
		capture.Endpoint.Name = endpoint
		capture.Endpoint.Config.Stream = str.Name
		captures = append(captures, capture)
	}

	var enc = yaml.NewEncoder(w)

	if err := enc.Encode(struct {
		Endpoints   map[string]Endpoint
		Collections map[string]Collection
		Captures    []Capture
	}{
		map[string]Endpoint{
			"the/endpoint": {Image: s.image, Config: s.config},
		},
		collections,
		captures,
	}); err != nil {
		return err
	}
	return enc.Close()
}

func inferFromJSONSchema(spec json.RawMessage) (*bindings.BuiltCatalog, error) {
	tmpdir, err := ioutil.TempDir("", "flow-discover")
	if err != nil {
		return nil, fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	var tmpfile = filepath.Join(tmpdir, "schema.yaml")
	mbp.Must(ioutil.WriteFile(tmpfile, spec, 0600), "writing spec")

	built, err := buildCatalog(pf.BuildAPI_Config{
		CatalogPath: filepath.Join(tmpdir, "catalog.db"),
		Directory:   tmpdir,
		Source:      tmpfile,
		SourceType:  pf.ContentType_JSON_SCHEMA,
	})
	if err != nil {
		return nil, fmt.Errorf("parsing JSON schema spec: %w", err)
	}
	return built, nil
}

func (cmd cmdDiscover) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var state = &state{
		app:     tview.NewApplication(),
		streams: make(map[string]struct{}),
	}
	state.ctx, state.cancel = context.WithCancel(context.Background())

	////////////////// Page for selecting image.

	state.addSelectConnectorPage()
	if err := state.app.EnableMouse(true).Run(); err != nil {
		panic(err)
	}
	mbp.Must(state.err, "error")

	return nil
}

func getConnectorCatalog(image string, config interface{}) (*ConnectorCatalog, error) {
	tmpdir, err := ioutil.TempDir("", "flow-discover")
	if err != nil {
		return nil, fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	configBytes, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("serializing configuration: %w", err)
	}

	var tmpfile = filepath.Join(tmpdir, "config.json")
	mbp.Must(ioutil.WriteFile(tmpfile, configBytes, 0600), "writing config")

	// Check that credentials are valid.
	if err := invokeConnector(
		func(cr ConnectorRecord) error {
			if cr.Type == "LOG" {
				os.Stderr.Write(cr.Log)
			} else if cr.Type == "CONNECTION_STATUS" {
				if cr.ConnectionStatus.Status != "SUCCEEDED" {
					return fmt.Errorf("credentials check %s: %s", cr.ConnectionStatus.Status, cr.ConnectionStatus.Message)
				}
			} else {
				return fmt.Errorf("unexpected `check` record %s", cr.Type)
			}
			return nil
		},
		"docker",
		"run",
		"--rm",
		"--mount",
		fmt.Sprintf("type=bind,source=%s,target=/config.json", tmpfile),
		image,
		"check",
		"--config",
		"/config.json",
	); err != nil {
		return nil, err
	}

	// Fetch connector catalog.
	var catalog *ConnectorCatalog

	if err := invokeConnector(
		func(cr ConnectorRecord) error {
			if cr.Type == "LOG" {
				os.Stderr.Write(cr.Log)
			} else if cr.Type == "CATALOG" {
				catalog = &cr.Catalog
			} else {
				return fmt.Errorf("unexpected `discover` record %s", cr.Type)
			}
			return nil
		},
		"docker",
		"run",
		"--rm",
		"--mount",
		fmt.Sprintf("type=bind,source=%s,target=/config.json", tmpfile),
		image,
		"discover",
		"--config",
		"/config.json",
	); err != nil {
		return nil, err
	}

	return catalog, nil
}

func invokeConnector(cb func(ConnectorRecord) error, args ...string) error {
	var stderr bytes.Buffer
	var cmd = exec.Command(args[0], args[1:]...)

	cmd.Stdout = newConnectorStdout(cb)
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		os.Stderr.Write(stderr.Bytes())
		return fmt.Errorf("invoking connector: %w", err)
	} else if err := cmd.Stdout.(io.Closer).Close(); err != nil {
		return fmt.Errorf("closing connector stdout: %w", err)
	}
	return nil
}

func getConnectorSpec(image string) (json.RawMessage, error) {
	var records []ConnectorRecord
	var stderr bytes.Buffer

	var cmd = exec.Command(
		"docker",
		"run",
		"--rm",
		image,
		"spec",
	)
	cmd.Stdout = newConnectorStdout(func(cr ConnectorRecord) error {
		records = append(records, cr)
		return nil
	})
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("fetching connector spec: %w (%s)", err, stderr.String())
	} else if err := cmd.Stdout.(io.Closer).Close(); err != nil {
		return nil, fmt.Errorf("closing connector stdout: %w", err)
	}

	for _, rec := range records {
		if rec.Type == "SPEC" {
			return rec.Spec.ConnectionSpecification, nil
		} else {
			log.WithField("record", rec).Info("connector log")
		}
	}
	return nil, fmt.Errorf("connector didn't return a SPEC")
}

func newConnectorStdout(cb func(ConnectorRecord) error) io.WriteCloser {
	return &connectorRecords{cb: cb}
}

type connectorRecords struct {
	rem []byte
	cb  func(ConnectorRecord) error
}

func (r *connectorRecords) Write(p []byte) (int, error) {
	if len(r.rem) == 0 {
		r.rem = append([]byte(nil), p...) // Clone.
	} else {
		r.rem = append(r.rem, p...)
	}

	var ind = bytes.LastIndexByte(r.rem, '\n') + 1
	var chunk = r.rem[:ind]
	r.rem = r.rem[ind:]

	var dec = json.NewDecoder(bytes.NewReader(chunk))
	dec.DisallowUnknownFields()

	for {
		var rec ConnectorRecord

		if err := dec.Decode(&rec); err == io.EOF {
			return len(p), nil
		} else if err != nil {
			return len(p), fmt.Errorf("decoding connector record: %w", err)
		} else if err = r.cb(rec); err != nil {
			return len(p), err
		}
	}
}

func (r *connectorRecords) Close() error {
	if len(r.rem) != 0 {
		return fmt.Errorf("connector stdout closed without a final newline")
	}
	return nil
}

type ConnectorCatalog struct {
	Streams []struct {
		Name                string          `json:"name"`
		JSONSchema          json.RawMessage `json:"json_schema"`
		SupportedSyncModes  []string        `json:"supported_sync_modes"`
		SourceDefinedCursor bool            `json:"source_defined_cursor"`
	} `json:"streams"`
}

type ConnectorRecord struct {
	Type string `json:"type"`
	Spec struct {
		DocumentationUrl        string          `json:"documentationUrl"`
		ConnectionSpecification json.RawMessage `json:"connectionSpecification"`
	}
	Catalog          ConnectorCatalog `json:"catalog"`
	Log              json.RawMessage  `json:"log"`
	ConnectionStatus struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"connectionStatus"`
}
