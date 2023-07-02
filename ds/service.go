package ds

import "github.com/saint-yellow/baradb"

// DS represents a Redis data structure service
type DS struct {
	db *baradb.DB // DB engine
}

// NewDS initializes a Redis data strucure
func NewDS(opts baradb.DBOptions) (*DS, error) {
	db, err := baradb.LaunchDB(opts)
	if err != nil {
		return nil, err
	}
	ds := &DS{
		db: db,
	}
	return ds, nil
}

// Close closes a Redis data structure service.
//
// Actually, it only closes the DB engine of the service.
func (ds *DS) Close() error {
	return ds.db.Close()
}
