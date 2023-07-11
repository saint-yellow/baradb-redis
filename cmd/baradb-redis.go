package main

import (
	"github.com/saint-yellow/baradb"
	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb-redis/client"
	"github.com/saint-yellow/baradb-redis/ds"
	"github.com/saint-yellow/baradb-redis/server"
)

const address = "127.0.0.1:6378"

func main() {
	service, err := ds.New(baradb.DefaultDBOptions)
	if err != nil {
		panic(err)
	}

	rs := server.New(service)
	innerServer := redcon.NewServer(
		address,
		client.ExecuteClientCommand,
		rs.Accept,
		func(conn redcon.Conn, err error) {},
	)
	rs.Server = innerServer

	go rs.Listen()
	<-rs.Signal
	rs.Stop()
}
