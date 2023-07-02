package main

import (
	"github.com/saint-yellow/baradb"
	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb/redis/client"
	"github.com/saint-yellow/baradb/redis/ds"
	"github.com/saint-yellow/baradb/redis/server"
)

const address = "127.0.0.1:6378"

func main() {
	service, err := ds.NewDS(baradb.DefaultDBOptions)
	if err != nil {
		panic(err)
	}

	rs := server.NewRedisServer(nil)
	rs.DBs = make(map[int]*ds.DS)
	rs.DBs[0] = service
	server := redcon.NewServer(
		address,
		client.ExecuteClientCommand,
		rs.Accept,
		rs.Close,
	)
	rs.Server = server

	rs.Listen()
}
