/*
Package transform provides a client interceptor that transforms gRPC requests.
*/
package transform

import (
	"context"
	"errors"

	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
	"google.golang.org/grpc"
)

// QueryToLookupWithKeysOnly returns a new unary client interceptor that
// transforms a RunQuery request to a Lookup request with KeysOnly query.
func QueryToLookupWithKeysOnly() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method != "/google.datastore.v1.Datastore/RunQuery" {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		in := req.(*datastorepb.RunQueryRequest)

		query := in.GetQuery()
		if query == nil {
			// GQL not supported.
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if query.GetProjection() != nil {
			// Projection or KeysOnly query.
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		// Invoke KeysOnly query.
		query.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
		defer func() {
			query.Projection = nil
		}()
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
		out := reply.(*datastorepb.RunQueryResponse)

		result := out.GetBatch().GetEntityResults()
		if len(result) == 0 {
			// Not found.
			return nil
		}

		// Invoke Lookup.
		getReq := &datastorepb.LookupRequest{
			ProjectId:   in.ProjectId,
			ReadOptions: in.GetReadOptions(),
			Keys:        make([]*datastorepb.Key, len(result)),
		}
		for i, v := range result {
			getReq.Keys[i] = v.GetEntity().GetKey()
		}
		getReply := &datastorepb.LookupResponse{}
		if err := invoker(ctx, "/google.datastore.v1.Datastore/Lookup", getReq, getReply, cc, opts...); err != nil {
			return err
		}

		found := getReply.GetFound()
		if len(found) != len(result) {
			return errors.New("could not lookup entities with same length as keys")
		}

		// Set results.
		for i, v := range found {
			out.Batch.EntityResults[i].Entity = v.Entity
		}
		out.Batch.EntityResultType = datastorepb.EntityResult_FULL
		return nil
	}
}
