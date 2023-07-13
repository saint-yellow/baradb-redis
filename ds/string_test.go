package ds

import (
	"math"
	"os"
	"strconv"
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
	defer destroyDS(ds, testingDBOptions.Directory)

	assert.Nil(t, err)
	assert.NotNil(t, ds)
	assert.NotNil(t, ds.db)
}

func TestDS_Set(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer func() {
		ds.db.Close()
		os.RemoveAll(testingDBOptions.Directory)
	}()

	var err error
	err = ds.Set(utils.NewKey(114), utils.NewRandomValue(114), 0)
	assert.Nil(t, err)
	err = ds.Set(utils.NewKey(514), utils.NewRandomValue(514), time.Second*10)
	assert.Nil(t, err)
}

func TestDS_Get(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

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

func TestDS_SetNx(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var success bool
	var err error
	var value []byte

	err = ds.Set(utils.NewKey(114), utils.NewKey(114), 0)
	assert.Nil(t, err)
	value, err = ds.Get(utils.NewKey(114))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(114), value)

	success = ds.SetNx(utils.NewKey(114), utils.NewKey(1140))
	assert.False(t, success)
	value, err = ds.Get(utils.NewKey(114))
	assert.Nil(t, err)
	assert.NotEqualValues(t, utils.NewKey(1140), value)
	assert.EqualValues(t, utils.NewKey(114), value)

	success = ds.SetNx(utils.NewKey(514), utils.NewKey(514))
	assert.True(t, success)
	value, err = ds.Get(utils.NewKey(514))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(514), value)
}

func TestDS_StrLen(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var length int
	var value []byte
	var err error

	length = ds.StrLen([]byte("unknown"))
	assert.Equal(t, 0, length)

	ds.Set([]byte("114"), []byte("514"), 0)
	value, err = ds.Get([]byte("114"))
	assert.Nil(t, err)
	assert.EqualValues(t, []byte("514"), value)
	length = ds.StrLen([]byte("114"))
	assert.Equal(t, len(value), length)
}

func TestDS_Append(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var length int
	var err error

	key := []byte("key001")
	value1, value2 := []byte("value001"), []byte("value002")

	length, err = ds.Append(key, value1)
	assert.Nil(t, err)
	assert.Equal(t, len(value1), length)

	length, err = ds.Append(key, value2)
	assert.Nil(t, err)
	assert.Equal(t, len(value1)+len(value2), length)

	err = ds.db.Delete(key)
	assert.Nil(t, err)

	length, err = ds.Append(key, value2)
	assert.Nil(t, err)
	assert.Equal(t, len(value2), length)
}

func TestDS_GetDel(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value []byte
	var err error

	key := []byte("key001")

	value, err = ds.GetDel(key)
	assert.Nil(t, err)
	assert.Nil(t, value)

	err = ds.Set(key, []byte("value001"), 0)
	assert.Nil(t, err)

	value, err = ds.GetDel(key)
	assert.Nil(t, err)
	assert.EqualValues(t, []byte("value001"), value)

	value, err = ds.GetDel(key)
	assert.Nil(t, err)
	assert.Nil(t, value)
}

func TestDS_GetSet(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value []byte
	var err error

	key := []byte("key001")
	value1, value2 := []byte("value001"), []byte("value002")

	value, err = ds.GetSet(key, value1)
	assert.Nil(t, err)
	assert.Nil(t, value)

	value, err = ds.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, value1, value)

	value, err = ds.GetSet(key, value2)
	assert.Nil(t, err)
	assert.EqualValues(t, value1, value)

	value, err = ds.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, value2, value)
}

func TestDS_Incr(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value int64
	var err error

	key1, key2 := []byte("key001"), []byte("key002")
	value1, value2 := []byte("10"), []byte("abc")

	value, err = ds.Incr(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), value)

	err = ds.Set(key1, value1, 0)
	assert.Nil(t, err)

	value, err = ds.Incr(key1)
	assert.Nil(t, err)
	assert.EqualValues(t, int64(11), value)

	err = ds.Set(key2, value2, 0)
	assert.Nil(t, err)

	value, err = ds.Incr(key2)
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)

	err = ds.Set(key2, []byte(strconv.FormatInt(math.MaxInt64, 10)), 0)
	assert.Nil(t, err)

	value, err = ds.Incr(key2)
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)
}

func TestDS_IncrBy(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value int64
	var err error

	key1, key2 := []byte("key001"), []byte("key002")
	value1, value2 := []byte("10"), []byte("abc")

	value, err = ds.IncrBy(key1, []byte("2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), value)

	err = ds.Set(key1, value1, 0)
	assert.Nil(t, err)

	value, err = ds.IncrBy(key1, []byte("-2"))
	assert.Nil(t, err)
	assert.EqualValues(t, int64(8), value)

	err = ds.Set(key2, value2, 0)
	assert.Nil(t, err)

	value, err = ds.IncrBy(key2, []byte("2"))
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)

	err = ds.Set(key2, []byte(strconv.FormatInt(math.MaxInt64, 10)), 0)
	assert.Nil(t, err)

	value, err = ds.IncrBy(key2, []byte("2"))
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)
}

func TestDS_IncrByFloat(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value float64
	var err error

	key1, key2 := []byte("key001"), []byte("key002")
	value1, value2 := []byte("10"), []byte("abc")

	value, err = ds.IncrByFloat(key1, []byte("3.14"))
	assert.Nil(t, err)
	assert.Equal(t, float64(3.14), value)

	err = ds.Set(key1, value1, 0)
	assert.Nil(t, err)

	value, err = ds.IncrByFloat(key1, []byte("-0.2"))
	assert.Nil(t, err)
	assert.EqualValues(t, float64(9.8), value)

	value, err = ds.IncrByFloat(key1, value2)
	assert.ErrorIs(t, err, ErrInvalidFloat)
	assert.Equal(t, float64(0), value)

	err = ds.Set(key2, value2, 0)
	assert.Nil(t, err)

	value, err = ds.IncrByFloat(key2, []byte("2"))
	assert.ErrorIs(t, err, ErrInvalidFloat)
	assert.Equal(t, float64(0), value)

	err = ds.Set(key2, utils.Float64ToBytes(math.MaxFloat64), 0)
	assert.Nil(t, err)

	value, err = ds.IncrByFloat(key2, []byte("2.718"))
	assert.ErrorIs(t, err, ErrInvalidFloat)
	assert.Equal(t, float64(0), value)

	err = ds.Set(key2, utils.Float64ToBytes(-math.MaxFloat64), 0)
	assert.Nil(t, err)

	value, err = ds.IncrByFloat(key2, []byte("-2.718"))
	assert.ErrorIs(t, err, ErrInvalidFloat)
	assert.Equal(t, float64(0), value)
}

func TestDS_Decr(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value int64
	var err error

	key1, key2 := []byte("key001"), []byte("key002")
	value1, value2 := []byte("10"), []byte("abc")

	value, err = ds.Decr(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), value)

	err = ds.Set(key1, value1, 0)
	assert.Nil(t, err)

	value, err = ds.Decr(key1)
	assert.Nil(t, err)
	assert.EqualValues(t, int64(9), value)

	err = ds.Set(key2, value2, 0)
	assert.Nil(t, err)

	value, err = ds.Decr(key2)
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)

	err = ds.Set(key2, []byte(strconv.FormatInt(math.MinInt64, 10)), 0)
	assert.Nil(t, err)

	value, err = ds.Decr(key2)
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)
}

func TestDS_DecrBy(t *testing.T) {
	ds, _ := New(testingDBOptions)
	defer destroyDS(ds, testingDBOptions.Directory)

	var value int64
	var err error

	key1, key2 := []byte("key001"), []byte("key002")
	value1, value2 := []byte("10"), []byte("abc")

	value, err = ds.DecrBy(key1, []byte("2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(-2), value)

	err = ds.Set(key1, value1, 0)
	assert.Nil(t, err)

	value, err = ds.DecrBy(key1, []byte("-2"))
	assert.Nil(t, err)
	assert.EqualValues(t, int64(12), value)

	err = ds.Set(key2, value2, 0)
	assert.Nil(t, err)

	value, err = ds.DecrBy(key2, []byte("2"))
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)

	err = ds.Set(key2, []byte(strconv.FormatInt(math.MinInt64, 10)), 0)
	assert.Nil(t, err)

	value, err = ds.DecrBy(key2, []byte("2"))
	assert.ErrorIs(t, err, ErrInvalidInteger)
	assert.Equal(t, int64(0), value)
}
