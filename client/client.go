package client

import (
	"fmt"
	"strings"

	"github.com/saint-yellow/baradb"
	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb/redis/ds"
)

type RedisClient struct {
	DB *ds.DS
}

func ExecuteClientCommand(conn redcon.Conn, cmd redcon.Command) {
	commandName := strings.ToLower(string(cmd.Args[0]))
	client, _ := conn.Context().(*RedisClient)

	switch commandName {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("PONG!")
	default:
		commandHandler, ok := supportedCommands[commandName]
		if !ok {
			conn.WriteError(fmt.Sprintf("%s is unsupported Redis command", commandName))
			return
		}
		result, err := commandHandler(client.DB, cmd.Args[1:]...)
		if err != nil {
			if err == baradb.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(result)
	}
}
