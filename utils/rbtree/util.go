package rbtree

// Int is type of int
type Int int

// Less returns whether x(Int) is smaller than specified item
func (x Int) Less(than Item) bool {
	return x < than.(Int)
}

// String is type of string
type String string

// Less returns whether x(String) is smaller than specified item
func (x String) Less(than Item) bool {
	return x < than.(String)
}

type TimestampItem struct {
	Time  int64
	Value interface{}
}

// Less returns whether x(String) is smaller than specified item
func (x TimestampItem) Less(than Item) bool {
	return x.Time < than.(TimestampItem).Time
}
