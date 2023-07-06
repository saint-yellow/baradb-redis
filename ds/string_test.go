package ds

import (
	"os"
	"testing"
	"time"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

var testingDBOptions = baradb.DefaultDBOptions

// preparations for tests
func init() {
	testingDBOptions.Directory = "/tmp/baradb-redis"
	os.RemoveAll(testingDBOptions.Directory)
}

// destroyDS a teardown method for clearing resources after testing
func destroyDS(ds *DS, dir string) {
	ds.db.Close()
	os.RemoveAll(dir)
}

func TestDS_New(t *testing.T) {
	ds, err := New(testingDBOptions)
	defer destroyDS(ds, (testingDBOptions.Directory))

	assert.Nil(t, err)
	assert.NotNil(t, ds)
	assert.NotNil(t, ds.db)
}

func TestDS_Set(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer func() {
		ds.db.Close()
		os.RemoveAll((testingDBOptions.Directory))
	}()

	var err error
	err = ds.Set(utils.NewKey(114), utils.NewRandomValue(114), 0)
	assert.Nil(t, err)
	err = ds.Set(utils.NewKey(514), utils.NewRandomValue(514), time.Second*10)
	assert.Nil(t, err)
}

func TestDS_Get(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, (testingDBOptions.Directory))

	ds.Set(utils.NewKey(114), utils.NewKey(114), 0)
	ds.Set(utils.NewKey(514), utils.NewKey(514), time.Second*3)

	var err error
	var value []byte

	value, err = ds.Get(utils.NewKey(114))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(114), value)
	value, err = ds.Get(utils.NewKey(514))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(514), value)

	// Change the value of a key
	ds.Set(utils.NewKey(114), utils.NewKey(1140), 0)
	value, err = ds.Get(utils.NewKey(114))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(1140), value)

	// Wait a few seconds to make a key/value pair expired
	time.Sleep(time.Second * 4)
	value, err = ds.Get(utils.NewKey(114))
	assert.Nil(t, err)
	assert.NotNil(t, value)
	value, err = ds.Get(utils.NewKey(514))
	assert.Equal(t, ErrExpiredValue, err)
	assert.Nil(t, value)

	value, err = ds.Get(utils.NewKey(1919))
	assert.Equal(t, baradb.ErrKeyNotFound, err)
	assert.Nil(t, value)
}
