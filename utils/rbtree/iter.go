package rbtree

type TreeIter struct {
	stack   []*TreeNode
	current *TreeNode

	t *Tree
}

func NewTreeIter(t *Tree) *TreeIter {
	current := t.root

	return &TreeIter{
		current: current,
		stack:   []*TreeNode{},

		t: t,
	}
}

func (i *TreeIter) Next() (key int64, value interface{}) {
	if !i.HasNext() {
		return 0, nil
	}

	for node := i.current; node != i.t.sentinal; node = node.left {
		i.stack = append(i.stack, node)
	}

	i.current, i.stack = i.stack[len(i.stack)-1], i.stack[:len(i.stack)-1]
	resultKey, resultValue := i.current.key, i.current.data

	i.current = i.current.right
	return resultKey, resultValue
}

func (i *TreeIter) HasNext() bool {
	return i.current != i.t.sentinal || len(i.stack) > 0
}
