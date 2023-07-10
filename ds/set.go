package ds

import (
	"encoding/binary"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/index"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

// encode encodes a set internal key to a byte array
func (sk *setInternalKey) encode() []byte {
	buffer := make([]byte, len(sk.key)+8+len(sk.member)+4)

	// key
	index := 0
	copy(buffer[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buffer[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(sk.member)))

	return buffer
}

func decodeSetInternalKey(buffer []byte) *setInternalKey {
	p := uint32(len(buffer))

	// member size
	memberSize := binary.LittleEndian.Uint32(buffer[p-4:])
	p -= 4

	// member
	member := make([]byte, memberSize)
	copy(member, buffer[p-memberSize:p])
	p -= memberSize

	// version
	version := binary.LittleEndian.Uint64(buffer[p-8 : p])
	p -= 8

	// key
	key := make([]byte, p)
	copy(key, buffer[:p])

	sk := &setInternalKey{
		key:     key,
		version: int64(version),
		member:  member,
	}
	return sk
}

// SAdd redis SADD
func (ds *DS) SAdd(key []byte, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sk.encode()

	var ok bool
	_, err = ds.db.Get(encKey)
	if err == baradb.ErrKeyNotFound {
		wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
		md.size++
		wb.Put(key, encodeMetadata(md))
		wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember redis SISMEMBER
func (ds *DS) SIsMember(key, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = ds.db.Get(encKey)
	if err != nil {
		if err != baradb.ErrKeyNotFound {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// SMembers redis SMEMBERS
func (ds *DS) SMembers(key []byte) ([][]byte, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return nil, err
	}

	if md.size == 0 {
		return nil, nil
	}

	members := make([][]byte, 0)

	opts := index.DefaultIteratorOptions
	opts.Prefix = key
	iter := ds.db.NewItrerator(opts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		encKey := iter.Key()
		if len(encKey) == 0 || len(encKey) == len(key) {
			continue
		}
		sk := decodeSetInternalKey(encKey)
		members = append(members, sk.member)
	}

	return members, nil
}

// SRem redis SREM
func (ds *DS) SRem(key, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = ds.db.Get(encKey)
	if err == baradb.ErrKeyNotFound {
		return false, nil
	}

	wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
	md.size--
	wb.Put(key, encodeMetadata(md))
	wb.Delete(encKey)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}

// SCard redis SCARD
func (ds *DS) SCard(key []byte) uint32 {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return 0
	}
	return md.size
}
