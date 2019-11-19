/*
Package redis provides Redis cache that implements cache.Cacher interface.
*/
package redis

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"go.opencensus.io/trace"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

// Cache is an implementation of cache.Cacher by Redis.
type Cache struct {
	expiration time.Duration
	client     *redis.Client
}

// NewCache returns a new Cache with given expiration. If set to 0, each item
// has no expiration time.
func NewCache(expiration time.Duration, client *redis.Client) *Cache {
	return &Cache{
		expiration: expiration,
		client:     client,
	}
}

// GetMulti returns the values of the given keys as a slice of []byte. If the
// item is not found, the corresponding index of the return value will be nil.
func (c *Cache) GetMulti(ctx context.Context, keys []*datastorepb.Key) [][]byte {
	_, span := trace.StartSpan(ctx, "github.com/DeNA/cloud-datastore-interceptor/cache/redis.GetMulti")
	defer func() { span.End() }()

	key := make([]string, len(keys))
	for i, k := range keys {
		key[i] = keystr(k)
	}

	values, err := c.client.MGet(key...).Result()
	if err != nil {
		return nil
	}

	ret := make([][]byte, len(values))
	for i, v := range values {
		if v != nil {
			ret[i] = []byte(v.(string))
		}
	}
	return ret
}

// SetMulti sets the given keys and values to items.
func (c *Cache) SetMulti(ctx context.Context, keys []*datastorepb.Key, values [][]byte) {
	_, span := trace.StartSpan(ctx, "github.com/DeNA/cloud-datastore-interceptor/cache/redis.SetMulti")
	defer func() { span.End() }()

	c.client.Pipelined(func(pipe redis.Pipeliner) error {
		for i, k := range keys {
			pipe.Set(keystr(k), values[i], c.expiration)
		}
		return nil
	})
}

// DeleteMulti deletes items for the given keys.
func (c *Cache) DeleteMulti(ctx context.Context, keys []*datastorepb.Key) error {
	_, span := trace.StartSpan(ctx, "github.com/DeNA/cloud-datastore-interceptor/cache/redis.DeleteMulti")
	defer func() { span.End() }()

	key := make([]string, len(keys))
	for i, k := range keys {
		key[i] = keystr(k)
	}
	return c.client.Del(key...).Err()
}

func keystr(key *datastorepb.Key) string {
	path := key.GetPath()

	s := make([]string, 0, len(path)*2+1)
	if ns := key.GetPartitionId().GetNamespaceId(); ns != "" {
		s = append(s, ns)
	} else {
		s = append(s, "[default]")
	}
	for _, p := range path {
		s = append(s, p.GetKind())
		switch p.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			s = append(s, strconv.FormatInt(p.GetId(), 10))
		case *datastorepb.Key_PathElement_Name:
			s = append(s, strconv.Quote(p.GetName()))
		}
	}
	return strings.Join(s, "/")
}
