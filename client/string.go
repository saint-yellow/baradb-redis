package client

import (
	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb-redis/ds"
)

func set(ds *ds.DS, args ...[]byte) (any, error) {
	var err error
	if len(args) != 2 {
		err = newErrWrongNumberOfArguments("set")
		return nil, err
	}

	key, value := args[0], args[1]
	err = ds.Set(key, value, 0)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(ds *ds.DS, args ...[]byte) (any, error) {
	var err error
	if len(args) != 1 {
		err = newErrWrongNumberOfArguments("get")
		return nil, err
	}
	key := args[0]
	return ds.Get(key)
}

func setnx(ds *ds.DS, args ...[]byte) (any, error) {
	var err error
	if len(args) != 2 {
		err = newErrWrongNumberOfArguments("setnx")
		return nil, err
	}

	key, value := args[0], args[1]
	success := ds.SetNx(key, value)
	if !success {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func strlen(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("strlen")
	}

	key := args[0]
	length := ds.StrLen(key)
	return length, nil
}
