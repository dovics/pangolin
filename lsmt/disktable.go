package lsmt

import (
	"container/heap"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	db "github.com/dovics/pangolin"
	"github.com/dovics/pangolin/utils/lru"
)

var defaultFileCount int = 10

func (s *Storage) ReadDiskTableMeta() {
	path.Join(s.option.WorkDir)
}

type disktable struct {
	mutex   sync.Mutex
	size    int
	workDir string

	filesIndexMap map[string]int
	files         []*diskFile

	cache lru.Cache
}

func NewDiskTable(workDir string, fileCount int) (*disktable, error) {
	if fileCount == 0 {
		fileCount = defaultFileCount
	}

	entrys, err := os.ReadDir(workDir)
	if err != nil {
		return nil, err
	}

	dt := &disktable{
		workDir:       workDir,
		size:          fileCount,
		cache:         lru.NewLRUCache(fileCount),
		filesIndexMap: make(map[string]int, len(entrys)),
	}

	dt.files = make([]*diskFile, 0, len(entrys))
	heap.Init(dt)

	for _, entry := range entrys {
		fileName := entry.Name()
		minKey, maxKey, err := parseFileName(fileName)
		if err != nil {
			return nil, err
		}

		file := &diskFile{
			t:      dt,
			path:   path.Join(workDir, fileName),
			minKey: minKey,
			maxKey: maxKey,
		}

		heap.Push(dt, file)

		if err := dt.cache.Put(file.path, file); err != nil {
			return nil, err
		}
	}

	return dt, nil
}

func (d *disktable) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, file := range d.files {
		if file.data != nil {
			if err := file.Clean(); err != nil {
				return err
			}
		}
	}

	return nil
}

type diskFile struct {
	t      *disktable
	path   string
	minKey int64
	maxKey int64

	data    io.ReadSeekCloser
	indexes [db.TypeCount]map[string]*index
}

func (d *disktable) Len() int           { return len(d.files) }
func (d *disktable) Less(i, j int) bool { return d.files[i].minKey < d.files[j].minKey }
func (d *disktable) Swap(i, j int) {
	d.files[i], d.files[j] = d.files[j], d.files[i]
	d.filesIndexMap[d.files[i].path] = i
	d.filesIndexMap[d.files[j].path] = j
}

func (d *disktable) Push(x interface{}) {
	file := x.(*diskFile)
	d.filesIndexMap[file.path] = len(d.files)
	d.files = append(d.files, file)
}

func (d *disktable) Pop() interface{} {
	x := d.files[len(d.files)-1]
	delete(d.filesIndexMap, x.path)
	d.files = d.files[0 : len(d.files)-1]
	return x
}

func (d *disktable) AddFile(p string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	minKey, maxKey, err := parseFileName(path.Base(p))
	if err != nil {
		return err
	}

	file := &diskFile{
		t:      d,
		path:   p,
		minKey: minKey,
		maxKey: maxKey,
	}

	heap.Push(d, file)
	if err := d.cache.Put(file.path, file); err != nil {
		return err
	}

	return nil
}

func (d *disktable) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]db.KV, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	result := []db.KV{}

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

func (d *diskFile) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]db.KV, error) {
	d.t.cache.Visit(d.path)
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

	check := func(i *index) ([]db.KV, error) {
		if filter != nil && !db.ContainTags(i.index, filter.Tags) {
			return nil, nil
		}

		if i.max < uint32(startTime) || i.min > uint32(endTime) {
			return nil, nil
		}

		if _, err := d.data.Seek(int64(i.offset), io.SeekStart); err != nil {
			return nil, err
		}

		b, err := readBlock(d.data)
		if err != nil {
			return nil, err
		}

		return b.getRange(startTime, endTime), nil
	}

	result := []db.KV{}
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

func (d *diskFile) Clean() error {
	heap.Remove(d.t, d.t.filesIndexMap[d.path])
	if d.data != nil {
		if err := d.data.Close(); err != nil {
			return err
		}
	}

	return nil
}

func parseFileName(filename string) (int64, int64, error) {
	keyScope := strings.Split(filename, "-")
	start, err := strconv.ParseInt(keyScope[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err := strconv.ParseInt(keyScope[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}
