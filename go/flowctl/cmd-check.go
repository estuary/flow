package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdCheck struct {
	Diagnostics    mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Directory      string                `long:"directory" default:"." description:"Build directory"`
	Log            mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Network        string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to"`
	PersistCatalog string                `long:"persist-catalog" description:"Write the catalog to the filesystem with the given name"`
	Source         string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
}

func (cmd cmdCheck) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var buildID = newBuildID()
	var err = apiBuild{
		BuildID:    buildID,
		Directory:  cmd.Directory,
		Source:     cmd.Source,
		SourceType: "catalog",
		FileRoot:   "/",
		Network:    cmd.Network,
		TSGenerate: true,
		TSCompile:  false,
		TSPackage:  false,
	}.execute(context.Background())

	if cmd.PersistCatalog == "" {
		// Cleanup output database.
		defer func() { _ = os.Remove(filepath.Join(cmd.Directory, buildID)) }()
	} else {
		defer func() {
			_ = os.Rename(filepath.Join(cmd.Directory, buildID), filepath.Join(cmd.Directory, cmd.PersistCatalog))
		}()
	}

	return err
}

func newBuildID() string {
	var data [9]byte
	var _, err = rand.Read(data[:])
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(data[:])
}
