package client

import (
	"github.com/saint-yellow/baradb/redis/client/commands"
	"github.com/saint-yellow/baradb/redis/ds"
)

type commandHandler func(service *ds.DS, arguments ...[]byte) (any, error)

var supportedCommands = map[string]commandHandler{
	"set": commands.Set,
	"get": commands.Get,
}
