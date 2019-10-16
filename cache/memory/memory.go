/*
Package memory provides in-memory cache that implements cache.Cacher interface.
*/
package memory

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opencensus.io/trace"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

type item struct {
	value []byte
	exp   int64
}

// Cache is an implementation of cache.Cacher using map type with an expiration
// time for each item.
type Cache struct {
	mu         sync.RWMutex
	expiration time.Duration
	items      map[string]item
}

// NewCache returns a new Cache with given expiration. If set to 0, each item
// has no expiration time.
func NewCache(expiration time.Duration) *Cache {
	return &Cache{
		expiration: expiration,
		items:      make(map[string]item),
	}
}

// GetMulti returns the values of the given keys as a slice of []byte. If the
// item has expired, the corresponding index of the return value will be nil.
func (c *Cache) GetMulti(ctx context.Context, keys []*datastorepb.Key) [][]byte {
	_, span := trace.StartSpan(ctx, "github.com/daisuzu/cloud-datastore-interceptor/cache/memory.GetMulti")
	defer func() { span.End() }()

	c.mu.RLock()
	defer c.mu.RUnlock()

	ret := make([][]byte, len(keys))

	now := time.Now().UnixNano()
	for i, k := range keys {
		if v, ok := c.items[keystr(k)]; ok {
			if c.expiration == 0 || v.exp >= now {
				ret[i] = v.value
			}
		}
	}

	return ret
}

// SetMulti sets the given keys and values to items.
func (c *Cache) SetMulti(ctx context.Context, keys []*datastorepb.Key, values [][]byte) {
	_, span := trace.StartSpan(ctx, "github.com/daisuzu/cloud-datastore-interceptor/cache/memory.SetMulti")
	defer func() { span.End() }()

	c.mu.Lock()
	defer c.mu.Unlock()

	var exp int64
	if c.expiration != 0 {
		exp = time.Now().Add(c.expiration).UnixNano()
	}

	for i, k := range keys {
		c.items[keystr(k)] = item{value: values[i], exp: exp}
	}
}

// DeleteMulti deletes items for the given keys. The returned error is always
// nil.
func (c *Cache) DeleteMulti(ctx context.Context, keys []*datastorepb.Key) error {
	_, span := trace.StartSpan(ctx, "github.com/daisuzu/cloud-datastore-interceptor/cache/memory.DeleteMulti")
	defer func() { span.End() }()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, k := range keys {
		delete(c.items, keystr(k))
	}
	return nil
}

func keystr(key *datastorepb.Key) string {
	var b strings.Builder

	if ns := key.GetPartitionId().GetNamespaceId(); ns != "" {
		b.WriteString(ns)
	} else {
		b.WriteString("[default]")
	}
	for _, p := range key.GetPath() {
		b.WriteString(p.GetKind())
		switch p.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			b.WriteString(strconv.FormatInt(p.GetId(), 10))
		case *datastorepb.Key_PathElement_Name:
			b.WriteString(strconv.Quote(p.GetName()))
		}
	}

	return b.String()
}
