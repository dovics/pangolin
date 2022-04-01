package lsmt

import (
	"encoding/binary"
	"os"

	"github.com/dovics/db"
)

func (s *Storage) WriteHeader(file *os.File, offsetBuffer []uint32, lengthBuffer []uint32) error {
	i := 0
	for t := 0; t < int(db.TypeCount); t++ {
		for index, table := range s.memtables[t] {
			offset := 0
			buffer := make([]byte, 28+len(index))
			binary.BigEndian.PutUint32(buffer[offset:], uint32(len(index)))
			offset += 4

			copy(buffer[offset:], index)
			offset += len(index)

			binary.BigEndian.PutUint32(buffer[offset:], uint32(t))
			offset += 4

			binary.BigEndian.PutUint32(buffer[offset:], uint32(table.count))
			offset += 4

			binary.BigEndian.PutUint32(buffer[offset:], uint32(table.data.Max().GetKey()))
			offset += 4

			binary.BigEndian.PutUint32(buffer[offset:], uint32(table.data.Min().GetKey()))
			offset += 4

			binary.BigEndian.PutUint32(buffer[offset:], offsetBuffer[i])
			offset += 4

			binary.BigEndian.PutUint32(buffer[offset:], lengthBuffer[i])
			offset += 4

			if _, err := file.Write(buffer); err != nil {
				return err
			}

			i++
		}
	}

	return nil
}
