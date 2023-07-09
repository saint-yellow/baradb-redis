package ds

import (
	"testing"

	"github.com/saint-yellow/baradb"
	"github.com/stretchr/testify/assert"
)

func TestDS_Del(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var err error

	err = ds.Del([]byte("unknown"))
	assert.Nil(t, err)

	ds.Set([]byte("string-1"), []byte("value-1"), 0)
	err = ds.Del([]byte("string-1"))
	assert.Nil(t, err)

	ds.HSet([]byte("hash-1"), []byte("field-1"), []byte("value-1"))
	err = ds.Del([]byte("hash-1"))
	assert.Nil(t, err)

	ds.LPush([]byte("list-1"), []byte("element-1"))
	err = ds.Del([]byte("list-1"))
	assert.Nil(t, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	err = ds.Del([]byte("set-1"))
	assert.Nil(t, err)

	ds.ZAdd([]byte("zset-1"), 0, []byte("member-1"))
	err = ds.Del([]byte("zset-1"))
	assert.Nil(t, err)
}

func TestDS_Type(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var dt dataType
	var err error

	dt, err = ds.Type([]byte("unknown"))
	assert.Equal(t, byte(0), dt)
	assert.ErrorIs(t, err, baradb.ErrKeyNotFound)

	ds.Set([]byte("string-1"), []byte("value-1"), 0)
	dt, err = ds.Type([]byte("string-1"))
	assert.Nil(t, err)
	assert.Equal(t, String, dt)

	ds.HSet([]byte("hash-1"), []byte("field-1"), []byte("value-1"))
	dt, err = ds.Type([]byte("hash-1"))
	assert.Equal(t, Hash, dt)
	assert.Nil(t, err)

	ds.LPush([]byte("list-1"), []byte("element-1"))
	dt, err = ds.Type([]byte("list-1"))
	assert.Equal(t, List, dt)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	dt, err = ds.Type([]byte("set-1"))
	assert.Equal(t, Set, dt)
	assert.Nil(t, err)

	ds.ZAdd([]byte("zset-1"), 0, []byte("member-1"))
	dt, err = ds.Type([]byte("zset-1"))
	assert.Equal(t, ZSet, dt)
	assert.Nil(t, err)
}

func TestDS_Exists(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var dt dataType
	var err error
	var exists bool

	dt, err = ds.Type([]byte("unknown"))
	assert.Equal(t, byte(0), dt)
	assert.ErrorIs(t, err, baradb.ErrKeyNotFound)
	exists = ds.Exists([]byte("unknown"))
	assert.False(t, exists)

	ds.Set([]byte("string-1"), []byte("value-1"), 0)
	dt, err = ds.Type([]byte("string-1"))
	assert.Nil(t, err)
	assert.Equal(t, String, dt)
	exists = ds.Exists([]byte("string-1"))
	assert.True(t, exists)
	err = ds.Del([]byte("string-1"))
	assert.Nil(t, err)
	exists = ds.Exists([]byte("string-1"))
	assert.False(t, exists)

	ds.HSet([]byte("hash-1"), []byte("field-1"), []byte("value-1"))
	dt, err = ds.Type([]byte("hash-1"))
	assert.Equal(t, Hash, dt)
	assert.Nil(t, err)
	exists = ds.Exists([]byte("hash-1"))
	assert.True(t, exists)
	err = ds.Del([]byte("hash-1"))
	assert.Nil(t, err)
	exists = ds.Exists([]byte("hash-1"))
	assert.False(t, exists)

	ds.LPush([]byte("list-1"), []byte("element-1"))
	dt, err = ds.Type([]byte("list-1"))
	assert.Equal(t, List, dt)
	assert.Nil(t, err)
	exists = ds.Exists([]byte("list-1"))
	assert.True(t, exists)
	err = ds.Del([]byte("list-1"))
	assert.Nil(t, err)
	exists = ds.Exists([]byte("list-1"))
	assert.False(t, exists)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	dt, err = ds.Type([]byte("set-1"))
	assert.Equal(t, Set, dt)
	assert.Nil(t, err)
	exists = ds.Exists([]byte("set-1"))
	assert.True(t, exists)
	err = ds.Del([]byte("set-1"))
	assert.Nil(t, err)
	exists = ds.Exists([]byte("set-1"))
	assert.False(t, exists)

	ds.ZAdd([]byte("zset-1"), 0, []byte("member-1"))
	dt, err = ds.Type([]byte("zset-1"))
	assert.Equal(t, ZSet, dt)
	assert.Nil(t, err)
	exists = ds.Exists([]byte("zset-1"))
	assert.True(t, exists)
	err = ds.Del([]byte("zset-1"))
	assert.Nil(t, err)
	exists = ds.Exists([]byte("zset-1"))
	assert.False(t, exists)
}
