package mainboilerplate

import (
	log "github.com/sirupsen/logrus"
)

// LogConfig configures handling of application log events.
type LogConfig struct {
	Level  string `long:"level" env:"LEVEL" default:"warn" choice:"trace" choice:"debug" choice:"info" choice:"warn" choice:"error" choice:"fatal" description:"Logging level"`
	Format string `long:"format" env:"FORMAT" default:"text" choice:"json" choice:"text" choice:"color" description:"Logging output format"`
}

// InitLog configures the logger.
func InitLog(cfg LogConfig) {
	if cfg.Format == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	} else if cfg.Format == "text" {
		log.SetFormatter(&log.TextFormatter{})
	} else if cfg.Format == "color" {
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	}

	if lvl, err := log.ParseLevel(cfg.Level); err != nil {
		log.WithField("err", err).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}
}
