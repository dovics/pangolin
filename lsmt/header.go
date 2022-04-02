package lsmt

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dovics/db"
)

type Index struct {
	index  string
	t      db.ValueType
	count  uint32
	max    uint32
	min    uint32
	offset uint32
	length uint32
}

func (i *Index) Bytes() []byte {
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

func WriteHeader(w io.Writer, indexes []*Index) error {
	for _, index := range indexes {
		if _, err := w.Write(index.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func FindHeader(file *os.File) error {
	if _, err := file.Seek(0, os.SEEK_END-4); err != nil {
		return err
	}

	buffer := make([]byte, 8)
	if _, err := file.Read(buffer); err != nil {
		return err
	}

	headerOffset := binary.BigEndian.Uint32(buffer)

	if _, err := file.Seek(int64(headerOffset), os.SEEK_CUR); err != nil {
		return err
	}

	return nil
}

func ReadHeader(r io.Reader) ([]*Index, error) {
	indexes := []*Index{}

	var err error
	buffer := make([]byte, 4)
	for {
		if _, err = r.Read(buffer); err != nil {
			fmt.Print(1)
			break
		}

		indexLength := binary.BigEndian.Uint32(buffer)
		indexBuffer := make([]byte, indexLength)

		if _, err := r.Read(indexBuffer); err != nil {
			fmt.Print(2)
			break
		}

		index := &Index{index: string(indexBuffer)}

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(3, err)
			break
		}
		index.t = db.ValueType(binary.BigEndian.Uint32(buffer))

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(4)
			break
		}
		index.count = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(5)
			break
		}
		index.max = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(6)
			break
		}
		index.min = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(7)
			break
		}
		index.offset = binary.BigEndian.Uint32(buffer)

		if _, err := r.Read(buffer); err != nil {
			fmt.Print(8)
			break
		}
		index.length = binary.BigEndian.Uint32(buffer)

		indexes = append(indexes, index)
	}

	if err != io.EOF {
		return nil, err
	}

	return indexes, nil
}
