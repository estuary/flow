package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
	"gopkg.in/yaml.v3"
)

// Top-level config of the authorization server.
type Config struct {
	Cookie cfgCookie
	// Configuration for enabled OIDC providers.
	// Keys are the provider name, matching a goth.Provider implementation.
	OIDC map[string]struct {
		ClientID     string `yaml:"clientID"`
		ClientSecret string `yaml:"clientSecret"`
		CallbackURL  string `yaml:"callbackURL"`
		Scopes       []string
	}
	Tokens cfgTokens
}

type cmdToken struct {
	Role string `long:"role" default:"user" description:"Role of the token"`

	Issuer  string        `long:"issuer" default:"testing" description:"Issuer to emulate (e.g, 'google')"`
	Subject string        `long:"subject" default:"8675309" description:"Subject of the token"`
	MaxAge  time.Duration `long:"max-age" default:"24h" description:"Maximum age of token"`

	DisplayName   string   `long:"name" default:"Jane M Doe" description:"Full name for display"`
	Email         string   `long:"email" default:"jane@doe.com" description:"Email address"`
	FirstName     string   `long:"first-name" default:"Jane" description:"First name"`
	LastName      string   `long:"last-name" default:"Doe" description:"LastName"`
	Locale        string   `long:"locale" default:"en" description:"Locale, if known"`
	Organizations []string `long:"org" description:"Organizations"`

	cmdConfig
}

type cmdConfig struct {
	Config string `long:"config" required:"t" description:"Path to configuration file"`
	cfg    Config
}

func (cmd *cmdConfig) loadConfig() error {
	var in, err = os.Open(cmd.Config)
	if err != nil {
		return fmt.Errorf("opening config: %w", err)
	}

	var dec = yaml.NewDecoder(in)
	dec.KnownFields(true)

	if err = dec.Decode(&cmd.cfg); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}
	if err = cmd.cfg.Cookie.init(); err != nil {
		return fmt.Errorf("initializing cookie management: %w", err)
	}
	if err = cmd.cfg.Tokens.init(); err != nil {
		return fmt.Errorf("initializing JWT signing: %w", err)
	}

	return nil
}

func (cmd *cmdToken) Execute(args []string) error {
	if err := cmd.loadConfig(); err != nil {
		return err
	}

	var cred = credential{
		Issuer:  cmd.Issuer,
		Subject: cmd.Subject,
		Expires: time.Now().Add(cmd.MaxAge).Unix(),
	}
	cred.Ext.DisplayName = cmd.DisplayName
	cred.Ext.Email = cmd.Email
	cred.Ext.FirstName = cmd.FirstName
	cred.Ext.LastName = cmd.LastName
	cred.Ext.Locale = cmd.Locale
	cred.Ext.Organizations = cmd.Organizations

	var session, err = cmd.cfg.Tokens.buildSession(cred, cmd.Role)
	if err != nil {
		return err
	}

	var enc = json.NewEncoder(os.Stdout)
	enc.SetIndent(" ", " ")
	return enc.Encode(session)
}

func main() {
	var parser = flags.NewParser(nil, flags.Default)
	var err error

	_, err = parser.AddCommand("serve", "Serve the authentication server", "Serve the authentication server", new(cmdServe))
	if err != nil {
		log.Fatal(err)
	}
	_, err = parser.AddCommand("token", "Generate an access token", "Generate an access token", new(cmdToken))
	if err != nil {
		log.Fatal(err)
	}

	if _, err = parser.Parse(); err == nil {
		// Success
	} else if _, ok := err.(*flags.Error); ok {
		// Flags already prints a notification
	} else {
		log.Fatal(err)
	}
}
