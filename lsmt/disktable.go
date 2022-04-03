package lsmt

import (
	"io"
	"path"

	"github.com/dovics/db"
)

func (s *Storage) ReadDiskTableMeta() {
	path.Join(s.option.WorkDir)
}

type disktable struct {
	files []*diskFile
}

type diskFile struct {
	data   io.ReadSeeker
	minKey int64
	maxKey int64

	indexes map[string]*index
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

func (d *disktable) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	return nil, nil
}
