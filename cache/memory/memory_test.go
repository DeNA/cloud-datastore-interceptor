package memory

import (
	"context"
	"reflect"
	"testing"
	"time"

	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
)

func TestCache_GetMulti(t *testing.T) {
	type fields struct {
		expiration time.Duration
		items      map[string]item
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
				items: map[string]item{
					"[default]k1": {value: []byte{'a'}, exp: time.Now().Add(1 * time.Hour).UnixNano()},
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
				items: map[string]item{
					"[default]k1": {value: []byte{'a'}, exp: time.Now().Add(1 * time.Hour).UnixNano()},
					"[default]k2": {value: []byte{'b'}, exp: time.Now().Add(1 * time.Hour).UnixNano()},
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
			fields: fields{items: map[string]item{}},
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
				items: map[string]item{
					"[default]k1": {value: []byte{'a'}, exp: time.Now().UnixNano()},
					"[default]k2": {value: []byte{'b'}, exp: time.Now().UnixNano()},
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
				items: map[string]item{
					"[default]k2": {value: []byte{'b'}},
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
			c := &Cache{
				expiration: tt.fields.expiration,
				items:      tt.fields.items,
			}
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
		want map[string]item
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
			want: map[string]item{
				"[default]k1": {value: []byte{'a'}},
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
			want: map[string]item{
				"[default]k1": {value: []byte{'a'}},
				"[default]k2": {value: []byte{'b'}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCache(0)
			c.SetMulti(context.Background(), tt.args.keys, tt.args.values)
			if got := c.items; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cache.SetMulti() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCache_DeleteMulti(t *testing.T) {
	type fields struct {
		items map[string]item
	}
	type args struct {
		keys []*datastorepb.Key
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]item
		wantErr bool
	}{
		{
			name: "1 item",
			fields: fields{
				items: map[string]item{
					"[default]k1": {value: []byte{'a'}},
					"[default]k2": {value: []byte{'b'}},
				},
			},
			args: args{keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{{Kind: "k", IdType: &datastorepb.Key_PathElement_Id{Id: 1}}},
				},
			}},
			want: map[string]item{
				"[default]k2": {value: []byte{'b'}},
			},
		},
		{
			name: "2 items",
			fields: fields{
				items: map[string]item{
					"[default]k1": {value: []byte{'a'}},
					"[default]k2": {value: []byte{'b'}},
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
			want: map[string]item{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cache{items: tt.fields.items}
			if err := c.DeleteMulti(context.Background(), tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("Cache.DeleteMulti() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got := c.items; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cache.DeleteMulti() = %v, want %v", got, tt.want)
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
			want: `[default]kind1`,
		},
		{
			name: "name",
			args: args{key: &datastorepb.Key{
				Path: []*datastorepb.Key_PathElement{
					{Kind: "kind", IdType: &datastorepb.Key_PathElement_Name{Name: "name"}},
				},
			}},
			want: `[default]kind"name"`,
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
