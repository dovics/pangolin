package rbtree

//
// Red-Black tree properties:  http://en.wikipedia.org/wiki/Tree
//
//  1) A node is either red or black
//  2) The root is black
//  3) All leaves (NULL) are black
//  4) Both children of every red node are black
//  5) Every simple path from root to leaves contains the same number
//     of black nodes.
//

// Node of the rbtree has a pointer of the node of parent, left, right, also has own color and Item which client uses
type Node struct {
	Left   *Node
	Right  *Node
	Parent *Node
	Color  uint

	// for use by client.
	Item
}

const (
	// RED represents the color of the node is red
	RED = 0
	// BLACK represents the color of the node is black
	BLACK = 1
)

// Item has a method to compare items which is less
type Item interface {
	Less(than Item) bool
}

// Tree represents a Red-Black tree.
type Tree struct {
	sentinal *Node
	root     *Node
	count    uint
}

func less(x, y Item) bool {
	return x.Less(y)
}

// New returns an initialized Red-Black tree
func New() *Tree { return new(Tree).Init() }

// Init returns the initial of rbtree
func (t *Tree) Init() *Tree {
	node := &Node{nil, nil, nil, BLACK, nil}
	return &Tree{
		sentinal: node,
		root:     node,
		count:    0,
	}
}

func (t *Tree) leftRotate(x *Node) {
	// Since we are doing the left rotation, the right child should *NOT* nil.
	if x.Right == t.sentinal {
		return
	}

	//
	// The illation of left rotation
	//
	//          |                                  |
	//          X                                  Y
	//         / \         left rotate            / \
	//        α  Y       ------------->         X   γ
	//           / \                            / \
	//          β  γ                         α  β
	//
	// It should be note that during the rotating we do not change
	// the Nodes' color.
	//
	y := x.Right
	x.Right = y.Left
	if y.Left != t.sentinal {
		y.Left.Parent = x
	}
	y.Parent = x.Parent

	if x.Parent == t.sentinal {
		t.root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}

	y.Left = x
	x.Parent = y
}

func (t *Tree) rightRotate(x *Node) {
	// Since we are doing the right rotation, the left child should *NOT* nil.
	if x.Left == t.sentinal {
		return
	}

	//
	// The illation of right rotation
	//
	//          |                                  |
	//          X                                  Y
	//         / \         right rotate           / \
	//        Y   γ      ------------->         α  X
	//       / \                                    / \
	//      α  β                                 β  γ
	//
	// It should be note that during the rotating we do not change
	// the Nodes' color.
	//
	y := x.Left
	x.Left = y.Right
	if y.Right != t.sentinal {
		y.Right.Parent = x
	}
	y.Parent = x.Parent

	if x.Parent == t.sentinal {
		t.root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}

	y.Right = x
	x.Parent = y
}

func (t *Tree) insert(z *Node) *Node {
	x := t.root
	y := t.sentinal

	for x != t.sentinal {
		y = x
		if less(z.Item, x.Item) {
			x = x.Left
		} else if less(x.Item, z.Item) {
			x = x.Right
		} else {
			return x
		}
	}

	z.Parent = y
	if y == t.sentinal {
		t.root = z
	} else if less(z.Item, y.Item) {
		y.Left = z
	} else {
		y.Right = z
	}

	t.count++
	t.insertFixup(z)
	return z
}

func (t *Tree) insertFixup(z *Node) {
	for z.Parent.Color == RED {
		//
		// Howerver, we do not need the assertion of non-nil grandparent
		// because
		//
		//  2) The root is black
		//
		// Since the color of the parent is RED, so the parent is not root
		// and the grandparent must be exist.
		//
		if z.Parent == z.Parent.Parent.Left {
			// Take y as the uncle, although it can be sentinal, in that case
			// its color is BLACK
			y := z.Parent.Parent.Right
			if y.Color == RED {
				//
				// Case 1:
				// Parent and uncle are both RED, the grandparent must be BLACK
				// due to
				//
				//  4) Both children of every red node are black
				//
				// Since the current node and its parent are all RED, we still
				// in violation of 4), So repaint both the parent and the uncle
				// to BLACK and grandparent to RED(to maintain 5)
				//
				//  5) Every simple path from root to leaves contains the same
				//     number of black nodes.
				//
				z.Parent.Color = BLACK
				y.Color = BLACK
				z.Parent.Parent.Color = RED
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Right {
					//
					// Case 2:
					// Parent is RED and uncle is BLACK and the current node
					// is right child
					//
					// A left rotation on the parent of the current node will
					// switch the roles of each other. This still leaves us in
					// violation of 4).
					// The continuation into Case 3 will fix that.
					//
					z = z.Parent
					t.leftRotate(z)
				}
				//
				// Case 3:
				// Parent is RED and uncle is BLACK and the current node is
				// left child
				//
				// At the very beginning of Case 3, current node and parent are
				// both RED, thus we violate 4).
				// Repaint parent to BLACK will fix it, but 5) does not allow
				// this because all paths that go through the parent will get
				// 1 more black node. Then repaint grandparent to RED (as we
				// discussed before, the grandparent is BLACK) and do a right
				// rotation will fix that.
				//
				z.Parent.Color = BLACK
				z.Parent.Parent.Color = RED
				t.rightRotate(z.Parent.Parent)
			}
		} else { // same as then clause with "right" and "left" exchanged
			y := z.Parent.Parent.Left
			if y.Color == RED {
				z.Parent.Color = BLACK
				y.Color = BLACK
				z.Parent.Parent.Color = RED
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Left {
					z = z.Parent
					t.rightRotate(z)
				}
				z.Parent.Color = BLACK
				z.Parent.Parent.Color = RED
				t.leftRotate(z.Parent.Parent)
			}
		}
	}
	t.root.Color = BLACK
}

// Just traverse the node from root to left recursively until left is sentinal.
// The node whose left is sentinal is the node with minimum value.
func (t *Tree) min(x *Node) *Node {
	if x == t.sentinal {
		return t.sentinal
	}

	for x.Left != t.sentinal {
		x = x.Left
	}

	return x
}

// Just traverse the node from root to right recursively until right is sentinal.
// The node whose right is sentinal is the node with maximum value.
func (t *Tree) max(x *Node) *Node {
	if x == t.sentinal {
		return t.sentinal
	}

	for x.Right != t.sentinal {
		x = x.Right
	}

	return x
}

func (t *Tree) search(x *Node) *Node {
	p := t.root

	for p != t.sentinal {
		if less(p.Item, x.Item) {
			p = p.Right
		} else if less(x.Item, p.Item) {
			p = p.Left
		} else {
			break
		}
	}

	return p
}

//TODO: Need Document
func (t *Tree) successor(x *Node) *Node {
	if x == t.sentinal {
		return t.sentinal
	}

	// Get the minimum from the right sub-tree if it existed.
	if x.Right != t.sentinal {
		return t.min(x.Right)
	}

	y := x.Parent
	for y != t.sentinal && x == y.Right {
		x = y
		y = y.Parent
	}
	return y
}

//TODO: Need Document
func (t *Tree) delete(key *Node) *Node {
	z := t.search(key)

	if z == t.sentinal {
		return t.sentinal
	}
	ret := &Node{t.sentinal, t.sentinal, t.sentinal, z.Color, z.Item}

	var y *Node
	var x *Node

	if z.Left == t.sentinal || z.Right == t.sentinal {
		y = z
	} else {
		y = t.successor(z)
	}

	if y.Left != t.sentinal {
		x = y.Left
	} else {
		x = y.Right
	}

	// Even if x is sentinal, we do the assign. In that case all the sentinal nodes will
	// change from {nil, nil, nil, BLACK, nil} to {nil, nil, ADDR, BLACK, nil},
	// but do not worry about that because it will not affect the compare
	// between Node-X with Node-sentinal
	x.Parent = y.Parent

	if y.Parent == t.sentinal {
		t.root = x
	} else if y == y.Parent.Left {
		y.Parent.Left = x
	} else {
		y.Parent.Right = x
	}

	if y != z {
		z.Item = y.Item
	}

	if y.Color == BLACK {
		t.deleteFixup(x)
	}

	t.count--

	return ret
}

func (t *Tree) deleteFixup(x *Node) {
	for x != t.root && x.Color == BLACK {
		if x == x.Parent.Left {
			w := x.Parent.Right
			if w.Color == RED {
				w.Color = BLACK
				x.Parent.Color = RED
				t.leftRotate(x.Parent)
				w = x.Parent.Right
			}
			if w.Left.Color == BLACK && w.Right.Color == BLACK {
				w.Color = RED
				x = x.Parent
			} else {
				if w.Right.Color == BLACK {
					w.Left.Color = BLACK
					w.Color = RED
					t.rightRotate(w)
					w = x.Parent.Right
				}
				w.Color = x.Parent.Color
				x.Parent.Color = BLACK
				w.Right.Color = BLACK
				t.leftRotate(x.Parent)
				// this is to exit while loop
				x = t.root
			}
		} else { // the code below is has left and right switched from above
			w := x.Parent.Left
			if w.Color == RED {
				w.Color = BLACK
				x.Parent.Color = RED
				t.rightRotate(x.Parent)
				w = x.Parent.Left
			}
			if w.Left.Color == BLACK && w.Right.Color == BLACK {
				w.Color = RED
				x = x.Parent
			} else {
				if w.Left.Color == BLACK {
					w.Right.Color = BLACK
					w.Color = RED
					t.leftRotate(w)
					w = x.Parent.Left
				}
				w.Color = x.Parent.Color
				x.Parent.Color = BLACK
				w.Left.Color = BLACK
				t.rightRotate(x.Parent)
				x = t.root
			}
		}
	}
	x.Color = BLACK
}
