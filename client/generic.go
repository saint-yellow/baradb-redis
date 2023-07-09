package client

import "github.com/saint-yellow/baradb/redis/ds"

func del(ds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("del")
	}

	key := args[0]
	return nil, ds.Del(key)
}

func datatype(rds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("type")
	}

	key := args[0]
	dt, err := rds.Type(key)
	if err != nil {
		return nil, err
	}
	dtn := dataTypeName(dt)
	return dtn, nil
}

func exists(rds *ds.DS, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, newErrWrongNumberOfArguments("exists")
	}

	key := args[0]
	exists := rds.Exists(key)
	return exists, nil
}

func dataTypeName(dt byte) string {
	switch dt {
	case ds.String:
		return "string"
	case ds.Hash:
		return "hash"
	case ds.Set:
		return "set"
	case ds.List:
		return "list"
	case ds.ZSet:
		return "zset"
	default:
		return "unknown data type"
	}
}
