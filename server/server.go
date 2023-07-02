package server

import (
	"log"
	"sync"

	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb/redis/client"
	"github.com/saint-yellow/baradb/redis/ds"
)

type RedisServer struct {
	DBs    map[int]*ds.DS
	Server *redcon.Server
	mu     *sync.RWMutex
}

func NewRedisServer(server *redcon.Server) *RedisServer {
	rs := &RedisServer{
		DBs:    make(map[int]*ds.DS),
		Server: server,
		mu:     new(sync.RWMutex),
	}
	return rs
}

func (rs *RedisServer) Listen() {
	log.Println("server running, ready to accept connections")
	rs.Server.ListenAndServe()
}

func (rs *RedisServer) Accept(conn redcon.Conn) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	client := &client.RedisClient{
		DB: rs.DBs[0],
	}
	conn.SetContext(client)
	return true
}

func (rs *RedisServer) Close(conn redcon.Conn, err error) {
	for _, db := range rs.DBs {
		db.Close()
	}
	rs.Server.Close()
}
