package client

import "github.com/saint-yellow/baradb-redis/ds"

func lpush(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) < 2 {
		return nil, newErrWrongNumberOfArguments("lpush")
	}

	key := args[0]
	elements := args[1:]
	return ds.LPush(key, elements...)
}

func rpush(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) < 2 {
		return nil, newErrWrongNumberOfArguments("rpush")
	}

	key := args[0]
	elements := args[1:]
	return ds.RPush(key, elements...)
}

func llen(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("llen")
	}
	key := args[0]
	return ds.LLen(key)
}
