package pool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

var ErrClosed = errors.New("pool closed")

type Factory[T any] func(context.Context) (T, error)

type entry[T any] struct {
	elm  T
	time time.Time
}

type Option[T any] func(*pool[T])

// WithEvict set the evict callback
func WithEvict[T any](cb func(T)) Option[T] {
	return func(p *pool[T]) {
		p.evict = cb
	}
}

// WithAge defined element max age (millisecond)
func WithAge[T any](maxAge int64) Option[T] {
	return func(p *pool[T]) {
		p.maxAge = maxAge
	}
}

// WithSize defined max size of Pool
func WithSize[T any](maxSize int) Option[T] {
	return func(p *pool[T]) {
		p.ch = make(chan *entry[T], maxSize)
	}
}

// Pool is for GC, see New for detail
type Pool[T any] struct {
	*pool[T]
}

type pool[T any] struct {
	mu      sync.Mutex
	ch      chan *entry[T]
	factory Factory[T]
	evict   func(T)
	maxAge  int64
	closed  bool
}

func (p *pool[T]) GetContext(ctx context.Context) (T, error) {
	now := time.Now()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			var zero T
			return zero, ErrClosed
		}
		select {
		case item := <-p.ch:
			elm := item
			p.mu.Unlock()
			if p.maxAge != 0 && now.Sub(item.time).Milliseconds() > p.maxAge {
				if p.evict != nil {
					p.evict(elm.elm)
				}
				continue
			}

			return elm.elm, nil
		default:
			p.mu.Unlock()
			item, err := p.factory(ctx)
			if err != nil {
				return item, err
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				if p.evict != nil {
					p.evict(item)
				}
				var zero T
				return zero, ErrClosed
			}
			p.mu.Unlock()
			return item, nil
		}
	}
}

func (p *pool[T]) Get() (T, error) {
	return p.GetContext(context.Background())
}

func (p *pool[T]) Put(item T) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if p.evict != nil {
			p.evict(item)
		}
		return
	}

	e := &entry[T]{
		elm:  item,
		time: time.Now(),
	}

	select {
	case p.ch <- e:
		p.mu.Unlock()
		return
	default:
		p.mu.Unlock()
		// pool is full
		if p.evict != nil {
			p.evict(item)
		}
		return
	}
}

func (p *Pool[T]) Close() {
	p.pool.mu.Lock()
	if p.pool.closed {
		p.pool.mu.Unlock()
		return
	}
	p.pool.closed = true
	items := make([]T, 0, len(p.pool.ch))
	for {
		select {
		case item := <-p.pool.ch:
			items = append(items, item.elm)
		default:
			p.pool.mu.Unlock()
			for _, item := range items {
				if p.pool.evict != nil {
					p.pool.evict(item)
				}
			}
			return
		}
	}
}

func recycle[T any](p *Pool[T]) {
	p.Close()
}

func New[T any](factory Factory[T], options ...Option[T]) *Pool[T] {
	p := &pool[T]{
		ch:      make(chan *entry[T], 10),
		factory: factory,
	}

	for _, option := range options {
		option(p)
	}

	P := &Pool[T]{p}
	runtime.SetFinalizer(P, recycle[T])
	return P
}
