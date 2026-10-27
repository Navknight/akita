package gmmu

import (
	"container/list"

	"github.com/sarchlab/akita/v3/mem/vm"
)

const bitsPerLevel = 9

type pwcKey struct {
	pid    vm.PID
	level  int
	prefix uint64
}

type pwc struct {
	capacity  int
	numLevels int
	entries   map[pwcKey]*list.Element
	lru       *list.List // front is the most recently used

	Hits []uint64
}

func newPWC(capacity, numLevels int) *pwc {
	return &pwc{
		capacity:  capacity,
		numLevels: numLevels,
		entries:   make(map[pwcKey]*list.Element),
		lru:       list.New(),
		Hits:      make([]uint64, numLevels),
	}
}

func (c *pwc) key(pid vm.PID, vpn uint64, level int) pwcKey {
	shift := uint(bitsPerLevel * (c.numLevels - level))
	return pwcKey{pid: pid, level: level, prefix: vpn >> shift}
}

func (c *pwc) lookup(pid vm.PID, vpn uint64) int {
	if c.capacity == 0 {
		c.Hits[0]++
		return 0
	}

	for level := c.numLevels - 1; level >= 1; level-- {
		if e, ok := c.entries[c.key(pid, vpn, level)]; ok {
			c.lru.MoveToFront(e)
			c.Hits[level]++
			return level
		}
	}

	c.Hits[0]++
	return 0
}

func (c *pwc) insert(pid vm.PID, vpn uint64) {
	if c.capacity == 0 {
		return
	}

	for level := 1; level < c.numLevels; level++ {
		k := c.key(pid, vpn, level)
		if e, ok := c.entries[k]; ok {
			c.lru.MoveToFront(e)
			continue
		}

		c.entries[k] = c.lru.PushFront(k)
		if c.lru.Len() > c.capacity {
			oldest := c.lru.Back()
			c.lru.Remove(oldest)
			delete(c.entries, oldest.Value.(pwcKey))
		}
	}
}
