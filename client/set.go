package client

import "github.com/saint-yellow/baradb-redis/ds"

func sadd(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 2 {
		return nil, newErrWrongNumberOfArguments("sadd")
	}

	key, member := args[0], args[1]
	return ds.SAdd(key, member)
}

func sismember(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 2 {
		return nil, newErrWrongNumberOfArguments("sismember")
	}

	key, member := args[0], args[1]
	return ds.SIsMember(key, member)
}

func srem(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 2 {
		return nil, newErrWrongNumberOfArguments("srem")
	}

	key, member := args[0], args[1]
	return ds.SRem(key, member)
}

func smembers(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("smembers")
	}

	key := args[0]
	return ds.SMembers(key)
}

func scard(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("scard")
	}

	key := args[0]
	return ds.SCard(key), nil
}
