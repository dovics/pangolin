package lsmt

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/dovics/db"
)

type index struct {
	index  string
	t      db.ValueType
	count  uint32
	min    uint32
	max    uint32
	offset uint32
	length uint32
}

func (i *index) bytes() []byte {
	offset := 0
	buffer := make([]byte, 28+len(i.index))
	binary.BigEndian.PutUint32(buffer[offset:], uint32(len(i.index)))
	offset += 4

	copy(buffer[offset:], i.index)
	offset += len(i.index)

	binary.BigEndian.PutUint32(buffer[offset:], uint32(i.t))
	offset += 4

	binary.BigEndian.PutUint32(buffer[offset:], uint32(i.count))
	offset += 4

	binary.BigEndian.PutUint32(buffer[offset:], uint32(i.max))
	offset += 4

	binary.BigEndian.PutUint32(buffer[offset:], uint32(i.min))
	offset += 4

	binary.BigEndian.PutUint32(buffer[offset:], i.offset)
	offset += 4

	binary.BigEndian.PutUint32(buffer[offset:], i.length)
	offset += 4

	return buffer
}

func writeHeader(w io.Writer, indexes []*index) error {
	for _, index := range indexes {
		if _, err := w.Write(index.bytes()); err != nil {
			return err
		}
	}

	return nil
}

func findHeader(r io.ReadSeeker) error {
	if _, err := r.Seek(0, os.SEEK_END); err != nil {
		return err
	}

	if _, err := r.Seek(-4, os.SEEK_CUR); err != nil {
		return err
	}

	buffer := make([]byte, 4)
	if _, err := r.Read(buffer); err != nil {
		return err
	}

	headerOffset := binary.BigEndian.Uint32(buffer)

	if _, err := r.Seek(int64(headerOffset), os.SEEK_SET); err != nil {
		return err
	}

	return nil
}

func readHeader(r io.Reader) ([db.TypeCount]map[string]*index, error) {
	indexes := [db.TypeCount]map[string]*index{}
	for i := range indexes {
		indexes[i] = make(map[string]*index)
	}

	var err error
	buffer := make([]byte, 4)
	for {
		if _, err = r.Read(buffer); err != nil {
			break
		}

		indexLength := binary.BigEndian.Uint32(buffer)
		indexBuffer := make([]byte, indexLength)

		if _, err := r.Read(indexBuffer); err != nil {
			break
		}

		index := &index{index: string(indexBuffer)}

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.t = db.ValueType(binary.BigEndian.Uint32(buffer))

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.count = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.max = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.min = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.offset = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			break
		}
		index.length = binary.BigEndian.Uint32(buffer)

		indexes[index.t][index.index] = index
	}

	if err != io.EOF {
		return indexes, err
	}

	return indexes, nil
}
