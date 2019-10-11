/*
Package cache provides a client interceptor for caching of the Cloud Datastore.

Cached data is used only for the following non-transactional data retrieval
and is saved when it retrieved from the datastore without existing in the
cache.
	- Get
	- GetMulti

Cached data is deleted when data is updated or deleted, including
transaction.
	- Put/PutMulti(when key is not IncompleteKey)
	- Delete/DeleteMulti
	- Mutation(Update, Upsert, Delete)

Even if changes are applied to the datastore, the cache deletion may fail.
In that case, the data will be inconsistent until the datastore is rolled
back or the cache is deleted.

Note that RunInTransaction does not roll back when cache deletion fails.

The advantage of using an interceptor is that can be used without changing
the client of cloud.google.com/go/datastore. However, unlike packages that
wrap the client like github.com/mjibson/goon, it's not possible to provide
a mechanism such as calling methods without a key. In addition, it may not
work correctly if the protobuf definitions for the Datastore is changed.
*/
package cache

import (
	"context"

	"github.com/golang/protobuf/proto"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
	"google.golang.org/grpc"
)

// Cacher is the interface implemented by an object that manages the cache.
type Cacher interface {
	// GetMulti returns the values of the given keys as a slice of []byte.
	// The length of this slice must be the same as length of the keys.
	// If there are missing values, set corresponding elements to nil.
	// It returns nil when all values are missing.
	GetMulti(ctx context.Context, keys []*datastorepb.Key) [][]byte

	// SetMulti saves in the pair of keys and values for each element.
	// This method will be called even if ErrNoSuchEntity is returned by
	// the datastore client when uncached data is found.
	SetMulti(ctx context.Context, keys []*datastorepb.Key, values [][]byte)

	// DeleteMulti deletes values for the given keys. The error is used to
	// make the datastore call fail. Do not return an error if the key is
	// not found in the cache.
	DeleteMulti(ctx context.Context, keys []*datastorepb.Key) error
}

// UnaryClientInterceptor returns a new unary client interceptor that caches
// gRPC calls of the Cloud Datastore using Cacher.
func UnaryClientInterceptor(cacher Cacher) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		switch method {
		case "/google.datastore.v1.Datastore/Lookup":
			in := req.(*datastorepb.LookupRequest)
			if in.GetReadOptions().GetTransaction() != nil {
				// Don't use cache in transaction.
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			out := reply.(*datastorepb.LookupResponse)

			keys := in.GetKeys()
			found := make([]*datastorepb.EntityResult, 0, len(keys))
			var missing []*datastorepb.Key

			cached := cacher.GetMulti(ctx, keys)
			for i, v := range cached {
				if v == nil {
					missing = append(missing, keys[i])
					continue
				}

				var e datastorepb.EntityResult
				if err := proto.Unmarshal(v, &e); err != nil {
					missing = append(missing, keys[i])
					continue
				}
				found = append(found, &e)
			}
			if len(keys) == len(found) {
				// Found all data.
				out.Found = found
				return nil
			}
			if len(cached) == 0 {
				// Not found all data.
				missing = keys
			}

			// Retrieve missing from Datastore.
			in.Keys = missing
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				return err
			}
			in.Keys = keys // Restore keys.

			got := out.GetFound()
			if got == nil {
				// Not found all data in Datastore.
				out.Found = found
				return nil
			}
			out.Found = append(out.Found, found...)

			// Save cache.
			skeys := make([]*datastorepb.Key, 0, len(got))
			values := make([][]byte, 0, len(got))
			for _, v := range got {
				if b, err := proto.Marshal(v); err == nil {
					skeys = append(skeys, v.Entity.Key)
					values = append(values, b)
				}
			}
			cacher.SetMulti(ctx, skeys, values)

			return nil

		case "/google.datastore.v1.Datastore/Commit":
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				return err
			}

			in := req.(*datastorepb.CommitRequest)
			keys := make([]*datastorepb.Key, 0, len(in.GetMutations()))
			for _, v := range in.GetMutations() {
				switch op := v.GetOperation().(type) {
				case *datastorepb.Mutation_Update:
					keys = append(keys, op.Update.Key)
				case *datastorepb.Mutation_Upsert:
					keys = append(keys, op.Upsert.Key)
				case *datastorepb.Mutation_Delete:
					keys = append(keys, op.Delete)
				}
			}
			if len(keys) > 0 {
				return cacher.DeleteMulti(ctx, keys)
			}

			return nil
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
