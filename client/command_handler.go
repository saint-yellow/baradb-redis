package client

import (
	"github.com/saint-yellow/baradb/redis/ds"
)

// commandHandler is a wrapper of Redis commands
type commandHandler func(service *ds.DS, arguments ...[]byte) (any, error)

// supportedCommands registers available Redis command handlers
var supportedCommands = map[string]commandHandler{
	// commands available for all data types
	"del":    del,
	"type":   datatype,
	"exists": exists,

	// commands available for string only
	"set":    set,
	"get":    get,
	"setnx":  setnx,
	"strlen": strlen,

	// commands available for list only
	"llen":  llen,
	"lpush": lpush,
	"rpush": rpush,

	// commands available for set only
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"smembers":  smembers,
	"scard":     scard,
}
