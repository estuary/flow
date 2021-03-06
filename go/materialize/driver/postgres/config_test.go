package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresConfig(t *testing.T) {
	var validConfig = config{
		Host:     "post.toast",
		Port:     1234,
		User:     "youser",
		Password: "shmassword",
		DBName:   "namegame",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "postgres://youser:shmassword@post.toast:1234/namegame", uri)

	var minimal = validConfig
	minimal.Port = 0
	minimal.DBName = ""
	require.NoError(t, minimal.Validate())
	uri = minimal.ToURI()
	require.Equal(t, "postgres://youser:shmassword@post.toast", uri)

	var noHost = validConfig
	noHost.Host = ""
	require.Error(t, noHost.Validate(), "expected validation error")

	var noUser = validConfig
	noUser.User = ""
	require.Error(t, noUser.Validate(), "expected validation error")

	var noPass = validConfig
	noPass.Password = ""
	require.Error(t, noPass.Validate(), "expected validation error")
}
