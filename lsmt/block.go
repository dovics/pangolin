package lsmt

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/dovics/db"
	"github.com/dovics/db/compress"
	"github.com/dovics/db/utils/rbtree"
)

type block struct {
	data *rbtree.Tree

	valueType db.ValueType
	count     int
}

func (b *block) set(key int64, value interface{}) {
	b.count++
	b.data.Insert(rbtree.TimestampItem{Time: key, Value: value})
}

func (b *block) get(key int64) interface{} {
	return b.data.Search(rbtree.TimestampItem{Time: key}).Item.(rbtree.TimestampItem).Value
}

func (b *block) getRange(startTime, endTime int64) []interface{} {
	items := b.data.GetRange(rbtree.TimestampItem{Time: startTime}, rbtree.TimestampItem{Time: endTime})
	result := make([]interface{}, len(items))

	for i, item := range items {
		result[i] = item.(rbtree.TimestampItem).Value
	}

	return result
}

// +---------------+---------------+----------------+----------------+---------------+
// |               |               |                |                |               |
// |   valueType   |   timeLength  |     times      |   valueLength  |     values    |
// |               |               |                |                |               |
// +---------------+---------------+----------------+----------------+---------------+

func (b *block) writeBlock(w io.Writer) (int, error) {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(b.valueType))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	timeEncoder := compress.NewTimeEncoder(b.count)
	valueEncoder := NewEncoder(b.valueType, b.count)

	iter := rbtree.NewTreeIter(b.data)
	for item := iter.Next().(rbtree.TimestampItem); iter.HasNext(); item = iter.Next().(rbtree.TimestampItem) {
		timeEncoder.Write(item.Time)
		if err := valueEncoder.Write(item.Value); err != nil {
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

	return 4*3 + timeLength + valueLength, nil
}

func readBlock(reader io.Reader) (*block, error) {
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
	timeDecoder.Read()
	decoder.Read()

	tree := rbtree.New()
	count := 0
	for {
		tree.Insert(rbtree.TimestampItem{Time: timeDecoder.Read(), Value: decoder.Read()})
		count++

		if !decoder.Next() || !timeDecoder.Next() {
			break
		}
	}

	return &block{
		tree,
		db.ValueType(valueType),
		count,
	}, nil

}
