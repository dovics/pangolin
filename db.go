package db

type DB struct {
	uuid string
}

func OpenDB(UUID string) (*DB, error) {

	return &DB{
		UUID,
	}, nil
}

func (db *DB) InsertValue(time uint64, value interface{}) error {
	return nil
}

func (db *DB) GetRange(startTime, endTime uint64) ([][]byte, error) {
	return nil, nil
}

type Interface interface {
	InsertValue(time uint64, value interface{}) error
	GetRange(startTime, endTime uint64) ([]interface{}, error)
	Merge(db *DB)
}
