package commands

import (
	"github.com/tidwall/redcon"

	"github.com/saint-yellow/baradb/redis/ds"
)

func Set(ds *ds.DS, args ...[]byte) (any, error) {
	var err error
	if len(args) != 2 {
		err = newErrWrongNumberOfArguments("set")
		return nil, err
	}

	key, value := args[0], args[1]
	err = ds.Set(key, value, 0)
	return redcon.SimpleString("OK"), err
}

func Get(ds *ds.DS, args ...[]byte) (any, error) {
	var err error
	if len(args) != 1 {
		err = newErrWrongNumberOfArguments("get")
		return nil, err
	}
	key := args[0]
	return ds.Get(key)
}
