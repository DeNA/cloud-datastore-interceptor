package redis

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

const minExp = time.Millisecond

func TestCache_GetMulti(t *testing.T) {
	type fields struct {
		expiration time.Duration
		items      map[string][]byte
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
				items: map[string][]byte{
					"[default]/k/1": {'a'},
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
				items: map[string][]byte{
					"[default]/k/1": {'a'},
					"[default]/k/2": {'b'},
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
				expiration: minExp,
				items: map[string][]byte{
					"[default]/k/1": {'a'},
					"[default]/k/2": {'b'},
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
				items: map[string][]byte{
					"[default]/k/2": {'b'},
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
			client := redis.NewClient(&redis.Options{})
			defer client.FlushDB()

			if _, err := client.Pipelined(func(pipe redis.Pipeliner) error {
				for k, v := range tt.fields.items {
					pipe.Set(k, v, tt.fields.expiration)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if tt.fields.expiration == minExp {
				time.Sleep(minExp)
			}

			c := NewCache(tt.fields.expiration, client)
			if got := c.GetMulti(context.Background(), tt.args.keys); !reflect.DeepEqual(got, tt.want) {
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
			client := redis.NewClient(&redis.Options{})
			defer client.FlushDB()

			c := NewCache(0, client)
			c.SetMulti(context.Background(), tt.args.keys, tt.args.values)
			got, err := client.Keys("*").Result()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.args.keys) {
				t.Errorf("Items = %v, want %v", got, len(tt.args.keys))
			}
		})
	}
}

func TestCache_DeleteMulti(t *testing.T) {
	type fields struct {
		items map[string][]byte
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
			fields: fields{items: map[string][]byte{
				"[default]/k/1": {'a'},
				"[default]/k/2": {'b'},
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
			fields: fields{items: map[string][]byte{
				"[default]/k/1": {'a'},
				"[default]/k/2": {'b'},
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
			fields: fields{items: map[string][]byte{
				"[default]/k/2": {'b'},
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
			client := redis.NewClient(&redis.Options{})
			defer client.FlushDB()

			if _, err := client.Pipelined(func(pipe redis.Pipeliner) error {
				for k, v := range tt.fields.items {
					pipe.Set(k, v, 0)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			c := NewCache(0, client)
			if err := c.DeleteMulti(context.Background(), tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("Cache.DeleteMulti() error = %v, wantErr %v", err, tt.wantErr)
			}
			got, err := client.Keys("*").Result()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tt.want {
				t.Errorf("Items = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keystr(t *testing.T) {
	type args struct {
		key *datastorepb.Key
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "id",
			args: args{key: &datastorepb.Key{
				Path: []*datastorepb.Key_PathElement{
					{Kind: "kind", IdType: &datastorepb.Key_PathElement_Id{Id: 1}},
				},
			}},
			want: `[default]/kind/1`,
		},
		{
			name: "name",
			args: args{key: &datastorepb.Key{
				Path: []*datastorepb.Key_PathElement{
					{Kind: "kind", IdType: &datastorepb.Key_PathElement_Name{Name: "name"}},
				},
			}},
			want: `[default]/kind/"name"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keystr(tt.args.key); got != tt.want {
				t.Errorf("keystr() = %v, want %v", got, tt.want)
			}
		})
	}
}
