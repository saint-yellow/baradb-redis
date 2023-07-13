package client

import (
	"github.com/saint-yellow/baradb-redis/ds"
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
	"append":      strappend,
	"decr":        decr,
	"decrby":      decrby,
	"get":         get,
	"getdel":      getdel,
	"getset":      getset,
	"incr":        incr,
	"incrby":      incrby,
	"incrbyfloat": incrbyfloat,
	"set":         set,
	"setnx":       setnx,
	"strlen":      strlen,

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
