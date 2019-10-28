/*
Package aememcache provides App Engine memcache cache that implements cache.Cacher interface.
*/
package aememcache

import (
	"context"
	"strconv"
	"strings"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

// Cache is an implementation of cache.Cacher by App Engine memcache.
type Cache struct {
	expiration time.Duration
}

// NewCache returns a new Cache with given expiration. If set to 0, each item
// has no expiration time.
func NewCache(expiration time.Duration) *Cache {
	return &Cache{
		expiration: expiration,
	}
}

// GetMulti returns the values of the given keys as a slice of []byte. If the
// item is not found, the corresponding index of the return value will be nil.
func (c *Cache) GetMulti(ctx context.Context, keys []*datastorepb.Key) [][]byte {
	key := make([]string, len(keys))
	keymap := make(map[string]int, len(keys))
	for i, k := range keys {
		ks := keystr(k)
		key[i] = ks
		keymap[ks] = i
	}

	items, err := memcache.GetMulti(ctx, key)
	if err != nil {
		log.Debugf(ctx, "memcache.GetMulti() err = %v", err)
		return nil
	}

	ret := make([][]byte, len(keys))
	for k, v := range items {
		ret[keymap[k]] = v.Value
	}
	return ret
}

// SetMulti sets the given keys and values to items.
func (c *Cache) SetMulti(ctx context.Context, keys []*datastorepb.Key, values [][]byte) {
	items := make([]*memcache.Item, len(keys))
	for i, k := range keys {
		items[i] = &memcache.Item{
			Key:        keystr(k),
			Value:      values[i],
			Expiration: c.expiration,
		}
	}
	if err := memcache.SetMulti(ctx, items); err != nil {
		log.Debugf(ctx, "memcache.SetMulti() err = %v", err)
	}
}

// DeleteMulti deletes items for the given keys. It returns an error if the
// target exists and could not be deleted.
func (c *Cache) DeleteMulti(ctx context.Context, keys []*datastorepb.Key) error {
	key := make([]string, len(keys))
	for i, k := range keys {
		key[i] = keystr(k)
	}

	err := memcache.DeleteMulti(ctx, key)
	if err != nil {
		log.Debugf(ctx, "memcache.DeleteMulti() err = %v", err)
	}
	merr, ok := err.(appengine.MultiError)
	if !ok {
		// Success or other error.
		return err
	}
	for _, e := range merr {
		if e == memcache.ErrServerError {
			return merr
		}
	}
	// Ignore memcache.ErrCacheMiss.
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
