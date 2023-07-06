package client

import (
	"github.com/saint-yellow/baradb/redis/ds"
)

// commandHandler is a wrapper of Redis commands
type commandHandler func(service *ds.DS, arguments ...[]byte) (any, error)

// supportedCommands registers available Redis command handlers
var supportedCommands = map[string]commandHandler{
	// commands available for all data types
	"type": datatype,

	// commands available for string only
	"set": set,
	"get": get,

	// commands available for list only
	"llen":  llen,
	"lpush": lpush,
	"rpush": rpush,
}
