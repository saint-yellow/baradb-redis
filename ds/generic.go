package ds

// Del redis DEL
func (ds *DS) Del(key []byte) error {
	return ds.db.Delete(key)
}

// Type redis TYPE
//
// It gets the type of the corresponding value of the given key
func (ds *DS) Type(key []byte) (dataType, error) {
	value, err := ds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(value) == 0 {
		return 0, ErrNilValue
	}

	dt := value[0]
	return dt, nil
}

// Exists redis EXISTS
func (ds *DS) Exists(key []byte) bool {
	_, err := ds.Type(key)
	if err != nil {
		return err == ErrNilValue
	}

	return true
}
