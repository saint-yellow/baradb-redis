package ds

import (
	"testing"

	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

func TestSetInternalKey_Decode(t *testing.T) {
	sk1 := &setInternalKey{
		key:     []byte("114"),
		version: 114514,
		member:  []byte("514"),
	}
	encKey := sk1.encode()
	sk2 := decodeSetInternalKey(encKey)
	assert.EqualValues(t, sk1, sk2)
	t.Log(sk1)
	t.Log(sk2)
}

func TestDS_SAdd(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var ok bool
	var err error
	var md *metadata

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 1)

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-1"))
	assert.False(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 1)

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-2"))
	assert.True(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 2)
}

func TestDS_SMembers(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var members [][]byte
	var err error

	members, err = ds.SMembers([]byte("unknown"))
	assert.Nil(t, err)
	assert.Nil(t, members)

	key := utils.NewKey(0)
	values := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		values[i] = utils.NewKey(i)
		ds.SAdd(key, values[i])
	}

	members, err = ds.SMembers(key)
	assert.Nil(t, err)
	assert.EqualValues(t, values, members)
}

func TestDS_SIsMember(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var ok bool
	var err error

	ok, err = ds.SIsMember([]byte("set-0"), []byte("member-0"))
	assert.False(t, ok)
	assert.Equal(t, nil, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-2"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-"), []byte("member-2"))
	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-2"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestDS_SRem(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var ok bool
	var err error

	ok, err = ds.SRem([]byte("set-0"), []byte("member-0"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))

	ok, err = ds.SRem([]byte("set-1"), []byte("member-0"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SRem([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-1"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestDS_SCard(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var count uint32 

	count = ds.SCard([]byte("unknown"))
	assert.Equal(t, uint32(0), count)

	key := []byte("key-1")
	for i := 1; i <= 10; i++ {
		ds.SAdd(key, utils.NewKey(i))
	}
	count = ds.SCard(key)
	assert.Equal(t, uint32(10), count)

	for i := 10; i >= 1; i-- {
		ds.SRem(key, utils.NewKey(i))
		assert.Equal(t, count-1, ds.SCard(key))
		count -= 1 
	}
}
