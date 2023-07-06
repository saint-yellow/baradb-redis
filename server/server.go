package server

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb/redis/client"
	"github.com/saint-yellow/baradb/redis/ds"
)

type RedisServer struct {
	DBs    map[int]*ds.DS
	Server *redcon.Server
	Signal chan os.Signal
	mu     *sync.RWMutex
}

func New(service *ds.DS) *RedisServer {
	rs := &RedisServer{
		DBs:    map[int]*ds.DS{
			0: service,
		},
		Signal: make(chan os.Signal, 1),
		mu:     new(sync.RWMutex),
	}
	signal.Notify(rs.Signal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return rs
}

func (rs *RedisServer) Listen() {
	log.Println("server running, ready to accept connections")
	if err := rs.Server.ListenAndServe(); err != nil {
		log.Fatalf("listen and serve err, fail to start. %v", err)
		return
	}
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

func (rs *RedisServer) Stop() {
	for _, db := range rs.DBs {
		if err := db.Close(); err != nil {
			log.Fatalf("close db err: %v", err)
		}
	}
	if err := rs.Server.Close(); err != nil {
		log.Fatalf("close server err: %v", err)
	}
	log.Println("baradb-redis is ready to exit, bye bye...")
}
