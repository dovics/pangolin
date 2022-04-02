package lsmt

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/dovics/db"
	"github.com/dovics/db/compress"
	"github.com/dovics/db/utils/rbtree"
)

type memtable struct {
	data *rbtree.Tree

	valueType db.ValueType
	count     int
}

func (m *memtable) Set(key int64, value interface{}) {
	m.count++
	m.data.Insert(key, value)
}

func (m *memtable) Get(key int64) interface{} {
	return m.data.Find(key).GetValue()
}

// +---------------+---------------+----------------+----------------+---------------+
// |               |               |                |                |               |
// |   valueType   |   timeLength  |     times      |   valueLength  |     values    |
// |               |               |                |                |               |
// +---------------+---------------+----------------+----------------+---------------+

func (m *memtable) Write(w io.Writer) (int, error) {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(m.valueType))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	timeEncoder := compress.NewTimeEncoder(m.count)
	valueEncoder := NewEncoder(m.valueType, m.count)

	iter := rbtree.NewTreeIter(m.data)
	for k, v := iter.Next(); iter.HasNext(); k, v = iter.Next() {
		timeEncoder.Write(k)
		if err := valueEncoder.Write(v); err != nil {
			return 0, err
		}
	}

	timeData, err := timeEncoder.Bytes()
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint32(buffer, uint32(len(timeData)))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	timeLength, err := w.Write(timeData)
	if err != nil {
		return 0, err
	}

	valueData, err := valueEncoder.Bytes()
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint32(buffer, uint32(len(valueData)))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	valueLength, err := w.Write(valueData)
	if err != nil {
		return 0, err
	}

	return timeLength + valueLength, nil
}

func Read(reader io.Reader) (*memtable, error) {
	buffer := make([]byte, 4)
	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	valueType := binary.BigEndian.Uint32(buffer)

	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	timeLength := binary.BigEndian.Uint32(buffer)

	timeDecoder := &compress.TimeDecoder{}

	timeBuffer := make([]byte, timeLength)
	if n, err := reader.Read(timeBuffer); err != nil {
		return nil, err
	} else if n != len(timeBuffer) {
		return nil, errors.New("no enough length")
	}

	timeDecoder.Init(timeBuffer)

	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	valueLength := binary.BigEndian.Uint32(buffer)

	valueBuffer := make([]byte, valueLength)
	if n, err := reader.Read(valueBuffer); err != nil {
		return nil, err
	} else if n != len(valueBuffer) {
		return nil, errors.New("no enough length")
	}

	decoder := NewDecoder(db.ValueType(valueType))
	decoder.SetBytes(valueBuffer)

	tree := rbtree.New()
	count := 0
	for {
		time := timeDecoder.Read()
		value := decoder.Read()
		tree.Insert(time, value)
		count++

		if !decoder.Next() || !timeDecoder.Next() {
			break
		}
	}

	return &memtable{
		tree,
		db.ValueType(valueType),
		count,
	}, nil

}
