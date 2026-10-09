package main

import (
	"sync"
	"sync/atomic"
)

type block struct {
	next atomic.Pointer[block]
	n    uint16
	data [topicBlockSize]string
}

var bp = sync.Pool{New: func() any { return new(block) }}

func gb() *block {
	x := bp.Get().(*block)
	x.next.Store(nil)
	x.n = 0
	return x
}

func pb(x *block) {
	x.next.Store(nil)
	x.n = 0
	bp.Put(x)
}

type prod struct {
	mu      sync.Mutex
	cur     *block
	n       uint16
	pubTail *block
	_       [32]byte
}

type cons struct {
	mu   sync.Mutex
	head *block
	pos  uint16
	_    [32]byte
}

type queue struct {
	p    prod
	c    cons
	stub *block
}

func newQ() *queue {
	stub := new(block)
	q := &queue{stub: stub}
	q.p.pubTail = stub
	q.c.head = stub
	return q
}

type ent struct {
	name string
	q    *queue
}

type GPTV6Broker struct {
	topics sync.Map
	first  atomic.Pointer[ent]
	multi  atomic.Bool
}

func NewGPTV6Broker() *GPTV6Broker { return &GPTV6Broker{} }

func (b *GPTV6Broker) lookup(t string, create bool) *queue {
	if !b.multi.Load() {
		if e := b.first.Load(); e != nil && e.name == t {
			return e.q
		}
	}
	if v, ok := b.topics.Load(t); ok {
		return v.(*ent).q
	}
	if !create {
		return nil
	}
	e := &ent{name: t, q: newQ()}
	v, l := b.topics.LoadOrStore(t, e)
	if l {
		return v.(*ent).q
	}
	if !b.first.CompareAndSwap(nil, e) {
		b.multi.Store(true)
	}
	return e.q
}

func (b *GPTV6Broker) Send(t, m string) {
	q := b.lookup(t, true)
	p := &q.p
	p.mu.Lock()
	x := p.cur
	if x == nil {
		x = gb()
		p.cur = x
		p.n = 0
	}
	x.data[p.n] = m
	p.n++
	if p.n == topicBlockSize {
		x.n = topicBlockSize
		p.cur = nil
		p.n = 0
		p.pubTail.next.Store(x)
		p.pubTail = x
	}
	p.mu.Unlock()
}

func (b *GPTV6Broker) Recv(t string) (string, error) {
	q := b.lookup(t, false)
	if q == nil {
		return "", ErrTopicNotFound
	}
	c := &q.c
	c.mu.Lock()
	if c.pos == 0 {
		if nx := c.head.next.Load(); nx != nil {
			old := c.head
			c.head = nx
			if old != q.stub {
				pb(old)
			}
		} else {
			p := &q.p
			p.mu.Lock()
			// A producer may have published a full block while we waited for p.mu.
			if nx := c.head.next.Load(); nx != nil {
				p.mu.Unlock()
				old := c.head
				c.head = nx
				if old != q.stub {
					pb(old)
				}
			} else {
				x := p.cur
				if x == nil || p.n == 0 {
					p.mu.Unlock()
					c.mu.Unlock()
					return "", ErrTopicEmpty
				}
				x.n = p.n
				p.cur = nil
				p.n = 0
				p.pubTail.next.Store(x)
				p.pubTail = x
				p.mu.Unlock()
				old := c.head
				c.head = x
				if old != q.stub {
					pb(old)
				}
			}
		}
	}
	x := c.head
	m := x.data[c.pos]
	x.data[c.pos] = ""
	c.pos++
	if c.pos == x.n {
		c.pos = 0
	}
	c.mu.Unlock()
	return m, nil
}
