package ds

import (
	"encoding/binary"
	"time"

	"github.com/saint-yellow/baradb"
)

// Set redis SET
func (ds *DS) Set(key []byte, value []byte, ttl time.Duration) error {
	if len(value) == 0 {
		return nil
	}

	// Encode value: type + expire + payload
	buffer := make([]byte, binary.MaxVarintLen64+1)
	buffer[0] = String
	index := 1

	// If the ttl is 0, then the key will not expire.
	var expire int64
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buffer[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buffer[:index])
	copy(encValue[index:], value)

	// Put the key and the encoded value to the DB engine
	return ds.db.Put(key, encValue)
}

// SetNx redis SETNX
func (ds *DS) SetNx(key []byte, value []byte) bool {
	_, err := ds.db.Get(key)
	if err == nil {
		return false
	}

	if err != baradb.ErrKeyNotFound {
		return false
	}

	err = ds.Set(key, value, 0)
	return err == nil
}

// Get redis GET
func (ds *DS) Get(key []byte) ([]byte, error) {
	encValue, err := ds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// Decode the encoded value
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, ErrExpiredValue
	}
	index += n
	payload := encValue[index:]
	return payload, nil
}

// GetDel redis GETDEL
func (ds *DS) GetDel(key []byte) ([]byte, error) {
	value, err := ds.Get(key)
	if err != nil {
		return nil, err
	}
	err = ds.Del(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// GetSet redis GETSET
func (ds *DS) GetSet(key, value []byte) ([]byte, error) {
	oldValue, _ := ds.Get(key)
	err := ds.Set(key, value, 0)
	if err != nil {
		return nil, err
	}
	return oldValue, nil
}

// StrLen redis STRLEN
func (ds *DS) StrLen(key []byte) int {
	value, err := ds.Get(key)
	if err != nil {
		return 0
	}
	return len(value)
}

// Append redis APPEND
func (ds *DS) Append(key, value []byte) (int, error) {
	exists := ds.Exists(key)
	if !exists {
		err := ds.Set(key, value, 0)
		if err != nil {
			return 0, err
		}
		return len(value), nil
	}

	oldValue, err := ds.Get(key)
	if err != nil {
		return 0, err
	}

	err = ds.Set(key, append(oldValue, value...), 0)
	if err != nil {
		return 0, err
	}
	return len(oldValue) + len(value), nil
}
