package transform

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/rpcreplay"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
	"google.golang.org/grpc"
)

var record = flag.Bool("record", false, "")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

type entity struct {
	S string
	I int
}

func putEntities(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	cli, err := datastore.NewClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	num := 100
	keys := make([]*datastore.Key, num)
	entities := make([]entity, num)
	for i := 0; i < num; i++ {
		keys[i] = datastore.IDKey("k", int64(i+1), nil)
		entities[i] = entity{S: strconv.Itoa(i), I: i % 10}
	}
	if _, err := cli.PutMulti(ctx, keys, entities); err != nil {
		t.Fatal(err)
	}
}

func invoker(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
	return cc.Invoke(ctx, method, req, reply, opts...)
}

func TestQueryToLookupWithKeysOnly(t *testing.T) {
	filename := filepath.Join("testdata", "TestQueryToLookupWithKeysOnly.replay")
	var conn *grpc.ClientConn
	if *record {
		putEntities(t)
		rec, err := rpcreplay.NewRecorder(filename, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rec.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		conn, err = grpc.Dial(os.Getenv("DATASTORE_EMULATOR_HOST"), append(rec.DialOptions(), grpc.WithInsecure())...)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		rep, err := rpcreplay.NewReplayer(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer rep.Close()
		conn, err = rep.Connection()
		if err != nil {
			t.Fatal(err)
		}
	}

	type args struct {
		method string
		req    interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "query all",
			args: args{
				method: "/google.datastore.v1.Datastore/RunQuery",
				req: &datastorepb.RunQueryRequest{
					ProjectId: "test",
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind: []*datastorepb.KindExpression{{Name: "k"}},
						},
					},
				},
			},
		},
		{
			name: "query limit=10",
			args: args{
				method: "/google.datastore.v1.Datastore/RunQuery",
				req: &datastorepb.RunQueryRequest{
					ProjectId: "test",
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind:  []*datastorepb.KindExpression{{Name: "k"}},
							Limit: &wrappers.Int32Value{Value: 10},
						},
					},
				},
			},
		},
		{
			name: "query offset=50",
			args: args{
				method: "/google.datastore.v1.Datastore/RunQuery",
				req: &datastorepb.RunQueryRequest{
					ProjectId: "test",
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind:   []*datastorepb.KindExpression{{Name: "k"}},
							Offset: 50,
						},
					},
				},
			},
		},
		{
			name: "query I>=8",
			args: args{
				method: "/google.datastore.v1.Datastore/RunQuery",
				req: &datastorepb.RunQueryRequest{
					ProjectId: "test",
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind: []*datastorepb.KindExpression{{Name: "k"}},
							Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{
								PropertyFilter: &datastorepb.PropertyFilter{
									Property: &datastorepb.PropertyReference{Name: "I"},
									Op:       datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL,
									Value:    &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 9}},
								},
							}},
						},
					},
				},
			},
		},
		{
			name: "query order by I desc",
			args: args{
				method: "/google.datastore.v1.Datastore/RunQuery",
				req: &datastorepb.RunQueryRequest{
					ProjectId: "test",
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind: []*datastorepb.KindExpression{{Name: "k"}},
							Order: []*datastorepb.PropertyOrder{
								{Property: &datastorepb.PropertyReference{Name: "I"}, Direction: datastorepb.PropertyOrder_DESCENDING},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			want := new(datastorepb.RunQueryResponse)
			invoker(ctx, tt.args.method, tt.args.req, want, conn)
			got := new(datastorepb.RunQueryResponse)
			QueryToLookupWithKeysOnly()(ctx, tt.args.method, tt.args.req, got, conn, invoker)
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(datastorepb.EntityResult{}, "Version")); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}
