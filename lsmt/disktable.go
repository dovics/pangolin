package lsmt

import (
	"container/heap"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/dovics/db"
)

func (s *Storage) ReadDiskTableMeta() {
	path.Join(s.option.WorkDir)
}

type disktable struct {
	mutex   sync.Mutex
	workDir string
	files   []*diskFile
}

func (d *disktable) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, file := range d.files {
		if file.data != nil {
			if err := file.data.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

func NewDiskTable(workDir string) (*disktable, error) {
	entrys, err := os.ReadDir(workDir)
	if err != nil {
		return nil, err
	}

	dt := &disktable{}

	dt.files = make([]*diskFile, 0, len(entrys))
	for _, entry := range entrys {
		fileName := entry.Name()
		keyScope := strings.Split(fileName, "-")

		minKey, err := strconv.Atoi(keyScope[0])
		if err != nil {
			return nil, err
		}

		maxKey, err := strconv.Atoi(keyScope[1])
		if err != nil {
			return nil, err
		}

		dt.files = append(dt.files,
			&diskFile{
				path:   path.Join(workDir, fileName),
				minKey: int64(minKey),
				maxKey: int64(maxKey),
			})
	}

	heap.Init(dt)

	return dt, nil
}

type diskFile struct {
	path   string
	minKey int64
	maxKey int64

	data    io.ReadSeekCloser
	indexes [db.TypeCount]map[string]*index
}

func (d *disktable) Len() int           { return len(d.files) }
func (d *disktable) Less(i, j int) bool { return d.files[i].minKey < d.files[j].minKey }
func (d *disktable) Swap(i, j int)      { d.files[i], d.files[j] = d.files[j], d.files[i] }

func (d *disktable) Push(x interface{}) {
	d.files = append(d.files, x.(*diskFile))
}

func (d *disktable) Pop() interface{} {
	x := d.files[len(d.files)-1]
	d.files = d.files[0 : len(d.files)-1]
	return x
}

func (d *disktable) AddFile(p string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	fileName := path.Base(p)
	keyScope := strings.Split(fileName, "-")

	minKey, err := strconv.Atoi(keyScope[0])
	if err != nil {
		return err
	}

	maxKey, err := strconv.Atoi(keyScope[1])
	if err != nil {
		return err
	}

	heap.Push(d, &diskFile{path: p, minKey: int64(minKey), maxKey: int64(maxKey)})

	return nil
}

func (d *disktable) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	result := []interface{}{}

	for _, file := range d.files {
		if file.minKey > endTime || file.maxKey < startTime {
			continue
		}

		fileResult, err := file.getRange(startTime, endTime, filter)
		if err != nil {
			return nil, err
		}

		result = append(result, fileResult...)
	}

	return result, nil
}

func (d *diskFile) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	if d.data == nil {
		file, err := os.Open(d.path)
		if err != nil {
			return nil, err
		}

		d.data = file
		if err := d.readIndex(); err != nil {
			return nil, err
		}
	}

	check := func(i *index) ([]interface{}, error) {
		if filter != nil && !db.ContainTags(i.index, filter.Tags) {
			return nil, nil
		}

		fmt.Println(i)
		if i.max < uint32(startTime) || i.min > uint32(endTime) {
			return nil, nil
		}

		if _, err := d.data.Seek(int64(i.offset), os.SEEK_SET); err != nil {
			return nil, err
		}

		b, err := readBlock(d.data)
		if err != nil {
			return nil, err
		}

		return b.getRange(startTime, endTime), nil
	}

	result := []interface{}{}
	if filter != nil && filter.Type != db.UnknownType {
		for _, index := range d.indexes[filter.Type] {
			r, err := check(index)
			if err != nil {
				return nil, err
			}

			result = append(result, r...)
		}

		return result, nil
	}

	for _, indexes := range d.indexes {
		for _, index := range indexes {
			r, err := check(index)
			if err != nil {
				return nil, err
			}

			result = append(result, r...)
		}
	}

	return result, nil
}

func (d *diskFile) readIndex() (err error) {
	if err := findHeader(d.data); err != nil {
		return err
	}

	if d.indexes, err = readHeader(d.data); err != nil {
		return err
	}

	return nil
}
