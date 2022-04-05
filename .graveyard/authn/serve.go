package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"
	"github.com/markbates/goth/providers/github"
	"github.com/markbates/goth/providers/gitlab"
	"github.com/markbates/goth/providers/google"
)

type cmdServe struct {
	Port uint16 `long:"port" default:"3000" description:"Port to serve on" env:"PORT"`
	cmdConfig
}

// Map of provider constructors that can be enabled through configuration.
var providerBuilders = []func(clientID, clientSecret, callbackURL string, scopes ...string) goth.Provider{
	func(a, b, c string, d ...string) goth.Provider { return google.New(a, b, c, d...) },
	func(a, b, c string, d ...string) goth.Provider { return github.New(a, b, c, d...) },
	func(a, b, c string, d ...string) goth.Provider { return gitlab.New(a, b, c, d...) },
}

func (m *cmdServe) Execute(args []string) error {
	if err := m.loadConfig(); err != nil {
		return err
	}

	// Determine which identity providers to enable.
	var providers []goth.Provider
outer:
	for name, cfg := range m.cfg.OIDC {
		// Look for a matching provider. This nested construction is a bit odd,
		// though harmless, because we must first instantiate the provider to
		// ask it what its name is (and thus whether it matches).
		for _, b := range providerBuilders {
			if p := b(cfg.ClientID, cfg.ClientSecret, cfg.CallbackURL, cfg.Scopes...); p.Name() == name {
				providers = append(providers, p)
				continue outer
			}
		}
		return fmt.Errorf("no registered provider %q", name)
	}

	// Configure goth with our configured providers and secure cookie store.
	gothic.Store = m.cfg.Cookie.store
	goth.UseProviders(providers...)

	var p = mux.NewRouter()
	// Called via return redirect from an OIDC provider.
	p.Path("/auth/{provider}/callback").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := m.providerCallback(w, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})
	// Initiates an authentication with a named provider.
	// Accepts an optional `next` query parameter which is a post-authentication redirect to issue.
	p.Path("/auth/{provider}").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := m.providerInitiate(w, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})
	// Returns the user's authentication session as JSON.
	p.Path("/session/tokens").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Methods", "GET,OPTIONS")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if origin, err := url.Parse(req.Header.Get("Origin")); err != nil || origin.Hostname() != "localhost" {
			w.Header().Set("Access-Control-Allow-Origin", "https://dashboard.estuary.dev")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", origin.String())
		}

		if req.Method == http.MethodOptions {
			return // Preflight request, no need to send a body.
		}

		var session, err = m.extractSession(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var enc = json.NewEncoder(w)
		enc.SetIndent("", " ")
		_ = enc.Encode(session)
	})
	// Returns the user's authentication session as accessible HTML.
	// TODO(johnny): Perhaps combine with /session/tokens and disambiguate through Accept?
	p.Path("/session").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var session, err = m.extractSession(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = template.Must(template.ParseFiles("templates/session.html")).Execute(w, session)
		if err != nil {
			log.Println("rendering session template:", err.Error())
		}
	})
	// Returns public keys of JWT signing credentials as a standard JSON Web Key Set.
	// See: https://datatracker.ietf.org/doc/html/rfc7517
	p.Path("/jwks").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var enc = json.NewEncoder(w)
		enc.SetIndent("", " ")
		_ = enc.Encode(m.cfg.Tokens.jsonWebKeySet())
	})
	// Allow the user to being a sign-in.
	// TODO(johnny): Perhaps combine with /session ?
	p.Path("/").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var err = template.Must(template.ParseFiles("templates/index.html")).Execute(w, false)
		if err != nil {
			log.Println("rendering index template:", err.Error())
		}
	})

	log.Printf("listening on localhost:%d\n", m.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", m.Port), p)
}

func (m *cmdServe) providerInitiate(w http.ResponseWriter, req *http.Request) error {
	var cookie, err = m.cfg.Cookie.store.Get(req, m.cfg.Cookie.Session)
	if err != nil {
		return fmt.Errorf("loading cookie store: %w", err)
	}

	for k, v := range req.URL.Query() {
		switch k {
		case "next":
			cookie.Values["n"] = v[0]
		default:
			return fmt.Errorf("unknown query parameter %q", k)
		}
	}

	if err = cookie.Save(req, w); err != nil {
		return fmt.Errorf("persisting secure cookie: %w", err)
	}

	gothic.BeginAuthHandler(w, req)
	return nil
}

func (m *cmdServe) providerCallback(w http.ResponseWriter, req *http.Request) error {
	var cookie, err = m.cfg.Cookie.store.Get(req, m.cfg.Cookie.Session)
	if err != nil {
		return fmt.Errorf("loading secure cookie: %w", err)
	}

	user, err := gothic.CompleteUserAuth(w, req)
	if err != nil {
		return fmt.Errorf("completing authentication: %w", err)
	}

	var cred, _ = cookie.Values["c"].(credential)
	cred.push(user)
	cred.prune(time.Now())
	cookie.Values["c"] = cred

	// If there isn't a "next" redirect, then redirect to /session status.
	var next, _ = cookie.Values["n"].(string)
	if next == "" {
		next = req.URL.ResolveReference(&url.URL{Path: "../../../session"}).String()
	}
	delete(cookie.Values, "n")

	if err = cookie.Save(req, w); err != nil {
		return fmt.Errorf("persisting secure cookie: %w", err)
	}

	http.Redirect(w, req, next, http.StatusFound)
	return nil
}

func (m *cmdServe) extractSession(req *http.Request) (session, error) {
	var cookie, err = m.cfg.Cookie.store.Get(req, m.cfg.Cookie.Session)
	if err != nil {
		return session{}, fmt.Errorf("loading secure cookie: %w", err)
	}

	var cred, _ = cookie.Values["c"].(credential)
	cred.prune(time.Now())

	session, err := m.cfg.Tokens.buildSession(cred, "api_user")
	if err != nil {
		return session, fmt.Errorf("creating token: %w", err)
	}
	return session, nil
}
