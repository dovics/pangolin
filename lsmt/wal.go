package lsmt

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	db "github.com/dovics/pangolin"
)

type wal struct {
	filePath string
	file     *os.File
}

func NewWAL(path string) (*wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return &wal{filePath: path, file: file}, nil
}

func (w *wal) Clear() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close the WAL file %s: %w", w.filePath, err)
	}

	wf, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open the file %s: %w", w.filePath, err)
	}

	w.file = wf
	return nil
}

func (w *wal) AppendEntry(entry db.Entry) error {
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(len(entryBytes)))

	if _, err := w.file.Write(buffer); err != nil {
		return err
	}

	if _, err := w.file.Write(entryBytes); err != nil {
		return err
	}

	return nil
}

func (w *wal) Load() (*memtable, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning: %w", err)
	}

	memtable := NewMemtable()
	lengthBuffer := make([]byte, 8)
	for {
		_, err := w.file.Read(lengthBuffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read: %w", err)
		}

		if err == io.EOF {
			return memtable, nil
		}
		entryBuffer := make([]byte, binary.BigEndian.Uint64(lengthBuffer))
		if _, err := w.file.Read(entryBuffer); err != nil {
			return nil, err
		}
		entry := &db.Entry{}
		if err := json.Unmarshal(entryBuffer, entry); err != nil {
			return nil, err
		}

		if err := memtable.insert(entry); err != nil {
			return nil, err
		}
	}

}
