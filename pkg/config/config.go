package config

import (
	"github.com/hanaboso/go-log/pkg/zap"
	"github.com/jinzhu/configor"

	log "github.com/hanaboso/go-log/pkg"
)

type (
	appType struct {
		Debug bool `env:"APP_DEBUG"`
	}

	configType struct {
		App *appType
	}
)

var (
	// Logger logger
	Logger log.Logger
	app    appType

	config = configType{
		App: &app,
	}
)

func init() {
	Logger = zap.NewLogger()
	if err := configor.Load(&config); err != nil {
		Logger.Fatal(err)
	}

	if app.Debug {
		Logger.SetLevel(log.DEBUG)
	} else {
		Logger.SetLevel(log.INFO)
	}
}
