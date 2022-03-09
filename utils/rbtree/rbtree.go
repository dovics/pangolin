package rbtree

type Tree struct {
	root     *TreeNode
	sentinal *TreeNode
}

type Color bool

const red Color = true
const black Color = false

func IsRed(node *TreeNode) bool {
	return node.color == red
}

func IsBlack(node *TreeNode) bool {
	return node.color == black
}

func Red(node *TreeNode) {
	node.color = red
}

func Black(node *TreeNode) {
	node.color = black
}

type TreeNode struct {
	key    int64
	left   *TreeNode
	right  *TreeNode
	parent *TreeNode
	color  Color
	data   interface{}
}

func New() *Tree {
	sentinal := &TreeNode{}
	return &Tree{
		root:     sentinal,
		sentinal: sentinal,
	}
}

func (t *Tree) Find(key int64) *TreeNode {
	temp := t.root
	for temp != nil {
		if temp.key == key {
			return temp

		}

		if temp.key < key {
			temp = temp.right
			continue
		}

		temp = temp.left
	}

	return nil
}

func (t *Tree) Insert(key int64, data interface{}) {
	root := t.root
	sentinal := t.sentinal

	if root == sentinal {
		node := &TreeNode{
			key:  key,
			data: data,

			parent: nil,
			left:   sentinal,
			right:  sentinal,
			color:  black,
		}

		t.root = node
		return
	}

	node := t.insertNode(key, data)

	for node != t.root && IsRed(node.parent) {
		if node.parent == node.parent.parent.left {
			temp := node.parent.parent.right

			if IsRed(temp) {
				Black(node.parent)
				Black(temp)
				Red(node.parent.parent)
				node = node.parent.parent
				continue
			}

			if node == node.parent.right {
				node = node.parent
				t.leftRotate(node)
			}

			Black(node.parent)
			Red(node.parent.parent)
			t.rightRotate(node.parent.parent)
			continue
		} else {
			temp := node.parent.parent.left

			if IsRed(temp) {
				Black(node.parent)
				Black(temp)
				Red(node.parent.parent)
				node = node.parent.parent
			} else {
				if node == node.parent.left {
					node = node.parent
					t.rightRotate(node)
				}

				Black(node.parent)
				Red(node.parent.parent)
				t.leftRotate(node.parent.parent)
			}
		}
	}

	Black(root)
}

func (t *Tree) Delete(key int64) {
	node := t.Find(key)
	if node == nil {
		return
	}

	var temp, subst *TreeNode
	if node.left == t.sentinal {
		temp = node.right
		subst = node
	} else if node.right == t.sentinal {
		temp = node.left
		subst = node
	} else {
		subst = t.min(node.right)
		temp = subst.right
	}

	if subst == t.root {
		t.root = temp
		Black(temp)

		node.left = nil
		node.right = nil
		node.parent = nil
		node.key = 0

		return
	}

	isRed := IsRed(subst)

	if subst == subst.parent.left {
		subst.parent.left = temp
	} else {
		subst.parent.right = temp
	}

	if subst == node {
		temp.parent = subst.parent
	} else {
		if subst.parent == node {
			temp.parent = subst
		} else {
			temp.parent = subst.parent
		}

		subst.left = node.left
		subst.right = node.right
		subst.parent = node.parent

		subst.color = node.color

		if node == t.root {
			t.root = subst
		} else {
			if node == node.parent.left {
				node.parent.left = subst
			} else {
				node.parent.right = subst
			}
		}

		if subst.left != t.sentinal {
			subst.left.parent = subst
		}

		if subst.right != t.sentinal {
			subst.right.parent = subst
		}
	}

	node.left = nil
	node.right = nil
	node.parent = nil
	node.key = 0

	if isRed {
		return
	}

	for temp != t.root && IsBlack(temp) {
		if temp == temp.parent.left {
			w := temp.parent.right

			if IsRed(w) {
				Black(w)
				Red(temp.parent)
				t.leftRotate(temp.parent)
				w = temp.parent.right
			}

			if IsBlack(w.left) && IsBlack(w.right) {
				Red(w)
				temp = temp.parent
			} else {
				if IsBlack(w.right) {
					Black(w.left)
					Red(w)
					t.rightRotate(w)
					w = temp.parent.right
				}

				w.color = temp.parent.color
				Black(temp.parent)
				Black(w.right)
				t.leftRotate(temp.parent)
				temp = t.root
			}
		} else {
			w := temp.parent.left

			if IsRed(w) {
				Black(w)
				Red(temp.parent)
				t.rightRotate(temp.parent)
				w = temp.parent.left
			}

			if IsBlack(w.left) && IsBlack(w.right) {
				Red(w)
				temp = temp.parent
			} else {
				if IsBlack(w.left) {
					Black(w.right)
					Red(w)
					t.leftRotate(w)
					w = temp.parent.left
				}

				w.color = temp.parent.color
				Black(temp.parent)
				Black(w.left)
				t.rightRotate(temp.parent)
				temp = t.root
			}
		}
	}

	Black(temp)
}

func (t *Tree) insertNode(key int64, data interface{}) *TreeNode {
	var p **TreeNode
	temp := t.root

	for {
		if temp.key > key {
			p = &temp.left
		} else {
			p = &temp.right
		}

		if *p == t.sentinal {
			break
		}

		temp = *p
	}

	*p = &TreeNode{
		key:  key,
		data: data,

		parent: temp,
		left:   t.sentinal,
		right:  t.sentinal,
		color:  red,
	}

	return *p
}

func (t *Tree) leftRotate(node *TreeNode) {
	temp := node.right
	node.right = temp.left

	if temp.left != t.sentinal {
		temp.left.parent = node
	}

	temp.parent = node.parent

	if node == t.root {
		t.root = temp
	} else if node == node.parent.left {
		node.parent.left = temp
	} else {
		node.parent.right = temp
	}

	temp.left = node
	node.parent = temp
}

func (t *Tree) rightRotate(node *TreeNode) {
	temp := node.left
	node.left = temp.right

	if temp.right != t.sentinal {
		temp.right.parent = node
	}

	temp.parent = node.parent

	if node == t.root {
		t.root = temp
	} else if node == node.parent.right {
		node.parent.right = temp
	} else {
		node.parent.left = temp
	}

	temp.right = node
	node.parent = temp
}

func (t *Tree) min(node *TreeNode) *TreeNode {
	for node.left != t.sentinal {
		node = node.left
	}

	return node
}
