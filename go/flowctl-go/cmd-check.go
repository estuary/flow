package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdCheck struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdCheck) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

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

	// Cleanup output database.
	defer func() { _ = os.Remove(filepath.Join(cmd.Directory, buildID)) }()

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
