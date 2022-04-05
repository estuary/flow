package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptoRand "crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"gopkg.in/yaml.v3"
)

// cfgTokens is configuration for signing and verification of JSON Web Tokens.
type cfgTokens struct {
	MaxAge time.Duration `yaml:"maxAge"`
	Keys   []cfgTokenKey `yaml:"keys"`
}

type cfgTokenKey struct {
	KeyID           string            `yaml:"keyID"`
	ECDSAPrivateKey string            `yaml:"ecdsaPrivateKey"`
	key             *ecdsa.PrivateKey // Parsed representation of ECDSAPrivateKey.
}

func (cfg *cfgTokens) init() error {
	// Default MaxAge to one day.
	if cfg.MaxAge == 0 {
		cfg.MaxAge = time.Hour * 24
	}

	// Generate a new, random private key if no key is present.
	if len(cfg.Keys) == 0 {

		if key, err := ecdsa.GenerateKey(elliptic.P256(), cryptoRand.Reader); err != nil {
			return fmt.Errorf("generating ECDSA key: %w", err)
		} else if b, err := x509.MarshalECPrivateKey(key); err != nil {
			return fmt.Errorf("marshalling ECDSA private key: %w", err)
		} else {
			cfg.Keys = append(cfg.Keys, cfgTokenKey{
				KeyID: fmt.Sprintf("%x",
					rand.New(rand.NewSource(time.Now().UnixMicro())).Intn(1<<16-1)),

				ECDSAPrivateKey: string(pem.EncodeToMemory(&pem.Block{
					Type:  "EC PRIVATE KEY",
					Bytes: b,
				})),
			})
		}

		var persist, err = yaml.Marshal(struct{ Tokens cfgTokens }{*cfg})
		if err != nil {
			panic(err)
		}
		log.Printf("generated a new token signing key. Persist with:\n%s\n",
			string(persist))
	}

	for k := range cfg.Keys {
		var err error
		cfg.Keys[k].key, err = jwt.ParseECPrivateKeyFromPEM([]byte(cfg.Keys[k].ECDSAPrivateKey))
		if err != nil {
			return fmt.Errorf("parsing ECDSAPrivateKey of key[%d]: %w", k, err)
		}
	}

	// Sanity-check signing and verifying a token.
	{
		var now = time.Now().Unix()
		token, err := cfg.signToken("invalid", "invalid", jwt.MapClaims{"foo": "bar"}, now, now+10)
		if err != nil {
			return fmt.Errorf("signing test key: %w", err)
		} else if err = cfg.verifyToken(token, nil); err != nil {
			return fmt.Errorf("verifying test key: %w", err)
		}
	}

	// Print out copious debugging on exactly what public key is being used.
	pub, err := x509.MarshalPKIXPublicKey(&cfg.Keys[0].key.PublicKey)
	if err != nil {
		return fmt.Errorf("marshalling public key: %w", err)
	}
	log.Printf("Configured to use signing key (kid=%s):\n%s",
		cfg.Keys[0].KeyID,
		string(pem.EncodeToMemory(&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: pub,
		})),
	)

	// See: https://datatracker.ietf.org/doc/html/rfc7517
	jwks, err := json.Marshal(cfg.jsonWebKeySet())
	if err != nil {
		return fmt.Errorf("marshalling jwk: %w", err)
	}
	log.Printf("... as JSON Web Key Set (JWKS):\n\t%s", string(jwks))
	log.Printf("... as quoted JWKS:\n\t%q", string(jwks))

	return nil
}

func (cfg *cfgTokens) jsonWebKeySet() interface{} {
	var jwks []map[string]interface{}

	for _, k := range cfg.Keys {
		var key = k.key.PublicKey

		jwks = append(jwks, map[string]interface{}{
			"alg":     jwt.SigningMethodES256.Name,
			"key_ops": []string{"verify"},
			"kid":     k.KeyID,
			"use":     "sig",

			"crv": key.Curve.Params().Name,
			"kty": "EC",
			"x":   strings.TrimRight(base64.URLEncoding.EncodeToString(key.X.Bytes()), "="),
			"y":   strings.TrimRight(base64.URLEncoding.EncodeToString(key.Y.Bytes()), "="),
		})
	}
	return map[string][]map[string]interface{}{"keys": jwks}
}

// session is a comprehensive detailing of a User's authentication session.
type session struct {
	AccessToken string     `json:"accessToken"`
	Credential  credential `json:"credential"`
	Expires     int64      `json:"expires"`
	IDToken     string     `json:"IDToken"`
	Role        string     `json:"role"`
	Subject     string     `json:"sub"`
}

func (cfg *cfgTokens) buildSession(cred credential, role string) (session, error) {
	if cred.Issuer == "" {
		return session{}, fmt.Errorf("no authenticated session credential")
	}

	var session = session{
		AccessToken: "",
		Credential:  cred,
		Expires:     cred.Expires,
		IDToken:     "",
		Role:        role,
		Subject:     fmt.Sprintf("%s|%s", cred.Issuer, cred.Subject),
	}

	// Use the sooner of |cred.expires| and the maximum allowed age.
	var now = time.Now()
	if e := now.Add(cfg.MaxAge).Unix(); session.Expires <= 0 || e < session.Expires {
		session.Expires = e
	}

	var err error
	session.IDToken, err = cfg.signToken(
		session.Role,
		session.Subject,
		jwt.MapClaims{"ext": cred.Ext},
		now.Unix(),
		session.Expires,
	)
	if err != nil {
		return session, fmt.Errorf("building ID token: %w", err)
	}

	session.AccessToken, err = cfg.signToken(
		session.Role,
		session.Subject,
		nil,
		now.Unix(),
		session.Expires,
	)
	if err != nil {
		return session, fmt.Errorf("building access token: %w", err)
	}

	return session, nil
}

func (cfg *cfgTokens) signToken(role, subject string, claims jwt.MapClaims, now, expires int64) (string, error) {
	if claims == nil {
		claims = make(jwt.MapClaims)
	}

	claims["aud"] = "api.estuary.dev"
	claims["exp"] = expires
	claims["iat"] = now
	claims["iss"] = "auth.estuary.dev"
	claims["kid"] = cfg.Keys[0].KeyID
	claims["role"] = role
	claims["sub"] = subject

	return jwt.NewWithClaims(jwt.SigningMethodES256, claims).SignedString(cfg.Keys[0].key)
}

func (cfg *cfgTokens) verifyToken(token string, claims jwt.MapClaims) error {
	if claims == nil {
		claims = make(jwt.MapClaims)
	}

	var _, err = jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected method: %s", t.Header["alg"])
		}

		// Look for a key matching ID `kid` from the claims.
		var kid = claims["kid"]
		for k := range cfg.Keys {
			if kid == cfg.Keys[k].KeyID {
				return &cfg.Keys[k].key.PublicKey, nil
			}
		}
		return nil, fmt.Errorf("unknown key (kid=%s)", kid)
	})

	if err != nil {
		return err
	}
	return nil
}
