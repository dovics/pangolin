package lru

type Cache struct {
	size       int
	capacity   int
	cache      map[string]*LinkedNode
	head, tail *LinkedNode
}

type LinkedNode struct {
	key   string
	value Cleanable

	prev, next *LinkedNode
}

type Cleanable interface {
	Clean() error
}

func initLinkedNode(key string, value Cleanable) *LinkedNode {
	return &LinkedNode{
		key:   key,
		value: value,
	}
}

func NewLRUCache(capacity int) Cache {
	l := Cache{
		cache:    map[string]*LinkedNode{},
		head:     initLinkedNode("", nil),
		tail:     initLinkedNode("", nil),
		capacity: capacity,
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

func (c *Cache) Get(key string) Cleanable {
	if _, ok := c.cache[key]; !ok {
		return nil
	}
	node := c.cache[key]
	c.moveToHead(node)
	return node.value
}

func (c *Cache) Visit(key string) {
	if _, ok := c.cache[key]; !ok {
		return
	}
	node := c.cache[key]
	c.moveToHead(node)
}

func (c *Cache) Put(key string, value Cleanable) error {
	if _, ok := c.cache[key]; !ok {
		node := initLinkedNode(key, value)
		c.cache[key] = node
		c.addToHead(node)
		c.size++
		if c.size > c.capacity {
			removed := c.removeTail()
			if err := removed.value.Clean(); err != nil {
				return err
			}
			delete(c.cache, removed.key)
			c.size--
		}
	} else {
		node := c.cache[key]
		node.value = value
		c.moveToHead(node)
	}

	return nil
}

func (c *Cache) addToHead(node *LinkedNode) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

func (c *Cache) removeNode(node *LinkedNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (c *Cache) moveToHead(node *LinkedNode) {
	c.removeNode(node)
	c.addToHead(node)
}

func (c *Cache) removeTail() *LinkedNode {
	node := c.tail.prev
	c.removeNode(node)
	return node
}
