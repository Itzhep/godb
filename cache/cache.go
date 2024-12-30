package cache

import (
    "container/list"
    "sync"
)

type Cache struct {
    capacity int
    items    map[string]*list.Element
    lru      *list.List
    mutex    sync.RWMutex
}

func (c *Cache) removeOldest() {
    oldest := c.lru.Back()
    if oldest != nil {
        c.lru.Remove(oldest)
        kv := oldest.Value.(*entry)
        delete(c.items, kv.key)
    }
}

type entry struct {
    key   string
    value interface{}
}

func NewCache(capacity int) *Cache {
    return &Cache{
        capacity: capacity,
        items:    make(map[string]*list.Element),
        lru:      list.New(),
    }
}

func (c *Cache) Get(key string) (interface{}, bool) {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    
    if ele, hit := c.items[key]; hit {
        c.lru.MoveToFront(ele)
        return ele.Value.(*entry).value, true
    }
    return nil, false
}

func (c *Cache) Set(key string, value interface{}) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    if ele, exists := c.items[key]; exists {
        c.lru.MoveToFront(ele)
        ele.Value.(*entry).value = value
        return
    }
    
    ele := c.lru.PushFront(&entry{key, value})
    c.items[key] = ele
    
    if c.lru.Len() > c.capacity {
        c.removeOldest()
    }
}