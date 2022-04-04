package rbtree

// func TestTreeIter(t *testing.T) {
// 	result := make([]interface{}, 500)
// 	m := make(map[int64]int64, 500)

// 	for i := int64(0); i < 500; i++ {
// 		result[i] = i
// 		m[i] = i
// 	}

// 	tree := New()
// 	for k, v := range m {
// 		tree.Insert(k, v)
// 	}

// 	expectKey := int64(0)
// 	iter := NewTreeIter(tree)

// 	for k, v := iter.Next(); iter.HasNext(); k, v = iter.Next() {
// 		if k != expectKey {
// 			t.Errorf("wrong key, expect: %d, get: %d", expectKey, k)
// 		}

// 		if result[k] != v {
// 			t.Error("wrong value")
// 		}
// 		expectKey++
// 	}
// }
