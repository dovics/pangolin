package blot

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	db "github.com/dovics/pangolin"
	"go.etcd.io/bbolt"
)

func init() {
	db.Register("blot", func(o interface{}) (db.Engine, error) {
		if o == nil {
			o = defaultOption
		}

		option, ok := o.(*Option)
		if !ok {
			return nil, errors.New("wrong option type")
		}

		bblotDB, err := bbolt.Open(option.Path, 0666, nil)
		if err != nil {
			return nil, err
		}

		return &Storage{
			option:        option,
			db:            bblotDB,
			unmarshalFunc: make(map[string]UnmarshalFunc),
		}, nil
	})
}

type Option struct {
	Path string
}

var defaultOption *Option = &Option{
	Path: "./blot",
}

type Storage struct {
	option        *Option
	db            *bbolt.DB
	unmarshalFunc map[string]UnmarshalFunc
}

type UnmarshalFunc func([]byte) (interface{}, error)

func (s *Storage) SetUnmarshalFunc(index string, f UnmarshalFunc) {
	s.unmarshalFunc[index] = f
}

func (s *Storage) Insert(e *db.Entry) error {
	typeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBytes, uint32(e.Type))
	index := e.Index()
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(e.Key))

	value, err := json.Marshal(e.Value)
	if err != nil {
		return err
	}

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		typeBucket, err := tx.CreateBucketIfNotExists(typeBytes)
		if err != nil {
			return err
		}

		bucket, err := typeBucket.CreateBucketIfNotExists([]byte(index))
		if err != nil {
			return err
		}

		if err := bucket.Put(key, value); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (s *Storage) GetRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	result := []interface{}{}

	rangeBucket := func(name []byte, b *bbolt.Bucket) error {
		if b == nil {
			return nil
		}

		if filter != nil && !db.ContainTags(string(name), filter.Tags) {
			return nil
		}

		if err := b.ForEach(func(k, v []byte) error {
			fmt.Println(k, string(v))
			key := binary.BigEndian.Uint32(k)
			if key >= uint32(startTime) && key < uint32(endTime) {
				if f, ok := s.unmarshalFunc[string(name)]; ok {
					value, err := f(v)
					if err == nil {
						result = append(result, value)
						return nil
					}
					log.Println("data unmarshal error: ", err)
				}

				result = append(result, v)
			}

			return nil
		}); err != nil {
			return err
		}

		return nil
	}

	if filter != nil && filter.Type != db.UnknownType {
		typeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(typeBytes, uint32(filter.Type))

		if err := s.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(typeBytes)
			if bucket != nil {
				if err := bucket.ForEach(func(k, v []byte) error {
					return rangeBucket(k, bucket.Bucket(k))
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return nil, err
		}

		return result, nil
	}

	typeBytes := make([]byte, 4)
	if err := s.db.View(func(tx *bbolt.Tx) error {
		for i := 0; i < int(db.TypeCount); i++ {
			binary.BigEndian.PutUint32(typeBytes, uint32(i))
			bucket := tx.Bucket(typeBytes)
			if bucket != nil {
				if err := bucket.ForEach(func(k, v []byte) error {
					return rangeBucket(k, bucket.Bucket(k))
				}); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}
