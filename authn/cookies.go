package main

import (
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/gorilla/sessions"
	"github.com/markbates/goth"
)

// cfgCookie is configuration for authoring and verifying secure cookies
type cfgCookie struct {
	Session string
	Keys    []struct {
		Auth    string
		Encrypt string
	}
	MaxAge     time.Duration `yaml:"maxAge"`
	RequireSSL bool          `yaml:"requireSSL"`

	store *sessions.CookieStore
}

func (m *cfgCookie) init() error {
	// Initialize our configured Cookie storage.
	var keyPairs [][]byte
	for i, key := range m.Keys {

		keyAuth, err := parseCookieSecret(key.Auth)
		if err != nil {
			return fmt.Errorf("key[%d].Auth: %w", i, err)
		}
		keyEnc, err := parseCookieSecret(key.Encrypt)
		if err != nil {
			return fmt.Errorf("key[%d].Encrypt: %w", i, err)
		}
		keyPairs = append(keyPairs, keyAuth, keyEnc)
	}

	m.store = sessions.NewCookieStore(keyPairs...)
	m.store.MaxAge(int(m.MaxAge.Seconds()))
	m.store.Options.Path = "/"
	m.store.Options.HttpOnly = true // HttpOnly should always be enabled
	m.store.Options.Secure = m.RequireSSL

	// Register with `gob` for encode/decode support from session cookies.
	gob.RegisterName("cred", credential{})

	return nil
}

// credential describes a third-party User credential.
type credential struct {
	// Fields present in access and ID tokens.
	Issuer  string `json:"iss"`
	Subject string `json:"sub"`
	Expires int64  `json:"exp"`

	// Fields present only in ID tokens.
	Ext struct {
		AvatarURL     string      `json:"avatarURL,omitempty"`
		DisplayName   string      `json:"displayName,omitempty"`
		Email         string      `json:"email,omitempty"`
		FirstName     string      `json:"firstName,omitempty"`
		LastName      string      `json:"lastName,omitempty"`
		Locale        string      `json:"locale,omitempty"`
		Organizations []string    `json:"orgs,omitempty"`
		Prev          *credential `json:"prev,omitempty"`
	} `json:"ext,omitempty"`
}

// push a goth.User into the credential.
func (cred *credential) push(user goth.User) {
	if cred.Issuer == "" {
		// Previous credential is not set.
	} else if cred.Issuer == user.Provider && cred.Subject == user.UserID {
		// This is an update of the present credential.
	} else {
		// Push an older credential onto the list.
		cred.Ext.Prev = &credential{
			Issuer:  cred.Issuer,
			Subject: cred.Subject,
			Expires: cred.Expires,
		}
	}

	cred.Issuer = user.Provider
	cred.Subject = user.UserID
	cred.Expires = user.ExpiresAt.Unix()

	cred.Ext.AvatarURL = user.AvatarURL
	cred.Ext.DisplayName = user.Name
	cred.Ext.Email = user.Email
	cred.Ext.FirstName = user.FirstName
	cred.Ext.LastName = user.LastName
	cred.Ext.Locale = ""
	cred.Ext.Organizations = []string(nil)

	// Google-provided locale.
	if l, ok := user.RawData["locale"].(string); ok {
		cred.Ext.Locale = l
	}
	// Google-provided suite organization.
	if hd, ok := user.RawData["hd"].(string); user.Provider == "google" && ok {
		cred.Ext.Organizations = []string{hd}
	}
}

func (cred *credential) prune(now time.Time) {
	if cred.Ext.Prev != nil && cred.Ext.Prev.Expires <= now.Unix() {
		cred.Ext.Prev = nil
	}
	if cred.Expires <= now.Unix() {
		*cred = credential{}
	}
}

func parseCookieSecret(s string) ([]byte, error) {
	const help = "Tip: try base64.urlsafe_b64encode(open('/dev/urandom', 'rb').read(32)) in python"

	var b, err = base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("secret is not base64-url encoded: %w. %s", err, help)
	} else if l := len(b); l != 32 {
		return nil, fmt.Errorf("secret is not exactly 32 bytes (len: %d). %s", l, help)
	}

	return b, nil
}
