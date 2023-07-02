package commands

import "github.com/saint-yellow/baradb/redis/ds"

func del(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("del")
	}

	key := args[0]
	return nil, ds.Del(key)
}
