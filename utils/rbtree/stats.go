package rbtree

// This file contains most of the methods that can be used
// by the user. Anyone who wants to look for some API about
// the rbtree, this is the right place.

// Len returns number of nodes in the tree.
func (t *Tree) Len() uint { return t.count }

// Insert func inserts a item as a new RED node
func (t *Tree) Insert(item Item) {
	if item == nil {
		return
	}

	// Always insert a RED node
	t.insert(&Node{t.sentinal, t.sentinal, t.sentinal, RED, item})
}

//InsertOrGet inserts or retrieves the item in the tree. If the
//item is already in the tree then the return value will be that.
//If the item is not in the tree the return value will be the item
//you put in.
func (t *Tree) InsertOrGet(item Item) Item {
	if item == nil {
		return nil
	}

	return t.insert(&Node{t.sentinal, t.sentinal, t.sentinal, RED, item}).Item
}

//Delete delete the item in the tree
func (t *Tree) Delete(item Item) Item {
	if item == nil {
		return nil
	}

	// The `color` field here is nobody
	return t.delete(&Node{t.sentinal, t.sentinal, t.sentinal, RED, item}).Item
}

//Get search for the specified items which is carried by a Node
func (t *Tree) Get(item Item) Item {
	if item == nil {
		return nil
	}

	// The `color` field here is nobody
	ret := t.search(&Node{t.sentinal, t.sentinal, t.sentinal, RED, item})
	if ret == nil {
		return nil
	}

	return ret.Item
}

func (t *Tree) GetRange(start, end Item) []Item {
	result := make([]Item, 0, 20)
	t.AscendRange(start, end, func(i Item) bool {
		result = append(result, i)
		return true
	})

	return result
}

// Search does only search the node which includes it node
//TODO: This is for debug, delete it in the future
func (t *Tree) Search(item Item) *Node {

	return t.search(&Node{t.sentinal, t.sentinal, t.sentinal, RED, item})
}

// Min return the item minimum one
func (t *Tree) Min() Item {
	x := t.min(t.root)

	if x == t.sentinal {
		return nil
	}

	return x.Item
}

// Max return the item maxmum one
func (t *Tree) Max() Item {
	x := t.max(t.root)

	if x == t.sentinal {
		return nil
	}

	return x.Item
}
