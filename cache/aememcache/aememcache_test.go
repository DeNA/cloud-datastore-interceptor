package aememcache

import (
	"context"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/memcache"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

var ctx context.Context

func TestMain(m *testing.M) {
	var done func()
	var err error
	ctx, done, err = aetest.NewContext()
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	done()
	os.Exit(code)
}

func TestCache_GetMulti(t *testing.T) {
	type fields struct {
		expiration time.Duration
		items      []*memcache.Item
	}
	type args struct {
		keys []*datastorepb.Key
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [][]byte
	}{
		{
			name: "found 1 item",
			fields: fields{
				expiration: 1 * time.Hour,
				items: []*memcache.Item{
					{Key: "[default]k1", Value: []byte{'a'}},
				},
			},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
			}},
			want: [][]byte{{'a'}},
		},
		{
			name: "found 2 items",
			fields: fields{
				expiration: 1 * time.Hour,
				items: []*memcache.Item{
					{Key: "[default]k1", Value: []byte{'a'}},
					{Key: "[default]k2", Value: []byte{'b'}},
				},
			},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
				},
			}},
			want: [][]byte{{'a'}, {'b'}},
		},
		{
			name:   "not found",
			fields: fields{},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
				},
			}},
			want: [][]byte{nil, nil},
		},
		{
			name: "expired",
			fields: fields{
				expiration: 1,
				items: []*memcache.Item{
					{Key: "[default]k1", Value: []byte{'a'}, Expiration: 1},
					{Key: "[default]k2", Value: []byte{'b'}, Expiration: 1},
				},
			},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
				},
			}},
			want: [][]byte{nil, nil},
		},
		{
			name: "1 found 1 not found",
			fields: fields{
				items: []*memcache.Item{
					{Key: "[default]k2", Value: []byte{'b'}},
				},
			},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
				},
			}},
			want: [][]byte{nil, {'b'}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer memcache.Flush(ctx)
			if len(tt.fields.items) > 0 {
				if err := memcache.SetMulti(ctx, tt.fields.items); err != nil {
					t.Fatal(err)
				}
			}
			c := NewCache(tt.fields.expiration)
			if got := c.GetMulti(ctx, tt.args.keys); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cache.GetMulti() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCache_SetMulti(t *testing.T) {
	type args struct {
		keys   []*datastorepb.Key
		values [][]byte
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "1 item",
			args: args{
				keys: []*datastorepb.Key{
					{
						Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
					},
				},
				values: [][]byte{{'a'}},
			},
		},
		{
			name: "2 items",
			args: args{
				keys: []*datastorepb.Key{
					{
						Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
					},
					{
						Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
					},
				},
				values: [][]byte{{'a'}, {'b'}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer memcache.Flush(ctx)
			c := NewCache(0)
			c.SetMulti(ctx, tt.args.keys, tt.args.values)
			stats, err := memcache.Stats(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if int(stats.Items) != len(tt.args.keys) {
				t.Errorf("Items = %v, want %v", stats.Items, len(tt.args.keys))
			}
		})
	}
}

func TestCache_DeleteMulti(t *testing.T) {
	type fields struct {
		items []*memcache.Item
	}
	type args struct {
		keys []*datastorepb.Key
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "1 item",
			fields: fields{items: []*memcache.Item{
				{Key: "[default]k1", Value: []byte{'a'}},
				{Key: "[default]k2", Value: []byte{'b'}},
			}},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
			}},
			want: 1,
		},
		{
			name: "2 items",
			fields: fields{items: []*memcache.Item{
				{Key: "[default]k1", Value: []byte{'a'}},
				{Key: "[default]k2", Value: []byte{'b'}},
			}},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 2}}},
				},
			}},
			want: 0,
		},
		{
			name: "not found",
			fields: fields{items: []*memcache.Item{
				{Key: "[default]k2", Value: []byte{'b'}},
			}},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
			}},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer memcache.Flush(ctx)
			if len(tt.fields.items) > 0 {
				if err := memcache.SetMulti(ctx, tt.fields.items); err != nil {
					t.Fatal(err)
				}
			}
			c := NewCache(0)
			if err := c.DeleteMulti(ctx, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("Cache.DeleteMulti() error = %v, wantErr %v", err, tt.wantErr)
			}
			stats, err := memcache.Stats(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if int(stats.Items) != tt.want {
				t.Errorf("Items = %v, want %v", stats.Items, tt.want)
			}
		})
	}
}
