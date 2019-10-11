package cache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/golang/protobuf/proto"
	"google.golang.org/api/option"
	datastorepb "google.golang.org/genproto/googleapis/datastore/v1"
	"google.golang.org/grpc"
)

var exampleCacher = Cacher(nil)

func ExampleUnaryClientInterceptor() {
	datastore.NewClient(context.Background(), "my-project",
		option.WithGRPCDialOption(grpc.WithUnaryInterceptor(UnaryClientInterceptor(exampleCacher))),
	)
}

func datastoreClientWithInterceptor(t *testing.T, cacher Cacher) *datastore.Client {
	t.Helper()

	client, err := datastore.NewClient(context.Background(), "",
		option.WithGRPCDialOption(grpc.WithUnaryInterceptor(UnaryClientInterceptor(cacher))),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

var (
	errDelCache         = errors.New("delete cache error")
	errRunInTransaction = errors.New("run in transaction error")
)

type entity struct {
	Data string
}

func TestGet(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name      string
		datastore map[string]entity
		mockValue []byte
		key       string
		want      entity
		wantErr   error
		wantSet   bool
	}{
		{
			name: "from datastore",
			datastore: map[string]entity{
				"ok": {Data: "OK"},
			},
			key:     "ok",
			want:    entity{Data: "OK"},
			wantSet: true,
		},
		{
			name:      "from cache",
			mockValue: marshalCache("TestGet/from_cache", "cache", "HIT"),
			key:       "cache",
			want:      entity{Data: "HIT"},
			wantSet:   false,
		},
		{
			name:    "no such entity",
			key:     "not exists",
			wantErr: datastore.ErrNoSuchEntity,
			wantSet: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{}
			if tt.mockValue != nil {
				m.values = [][]byte{tt.mockValue}
			}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			var dst entity
			err := client.Get(context.Background(), datastore.NameKey(t.Name(), tt.key, nil), &dst)
			if err != tt.wantErr {
				t.Fatalf("client.Get() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(dst, tt.want) {
				t.Errorf("client.Get() dst = %v, want %v", dst, tt.want)
			}
			if isSet := len(m.setKeys) > 0; isSet != tt.wantSet {
				t.Errorf("called cacher.SetMulti() is %t, want %t", isSet, tt.wantSet)
			}
		})
	}
}

func TestGetMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name       string
		datastore  map[string]entity
		mockValues [][]byte
		keys       []string
		want       []entity
		wantErr    error
		wantSet    []string
	}{
		{
			name: "datastore only",
			datastore: map[string]entity{
				"1": {Data: "aaa"},
				"2": {Data: "bbb"},
			},
			keys:    []string{"1", "2"},
			want:    []entity{{Data: "aaa"}, {Data: "bbb"}},
			wantSet: []string{"1", "2"},
		},
		{
			name: "cache only",
			mockValues: [][]byte{
				marshalCache("TestGetMulti/cache_only", "1", "aaa"),
				marshalCache("TestGetMulti/cache_only", "2", "bbb"),
			},
			keys: []string{"1", "2"},
			want: []entity{{Data: "aaa"}, {Data: "bbb"}},
		},
		{
			name:    "no such entity",
			keys:    []string{"1", "2"},
			want:    []entity{{}, {}},
			wantErr: datastore.MultiError{datastore.ErrNoSuchEntity, datastore.ErrNoSuchEntity},
		},
		{
			name: "mixed/datastore,no such entity,datastore",
			datastore: map[string]entity{
				"1": {Data: "aaa"},
				"3": {Data: "ccc"},
			},
			keys:    []string{"1", "2", "3"},
			want:    []entity{{Data: "aaa"}, {}, {Data: "ccc"}},
			wantErr: datastore.MultiError{nil, datastore.ErrNoSuchEntity, nil},
			wantSet: []string{"1", "3"},
		},
		{
			name: "mixed/cache,datastore,cache",
			datastore: map[string]entity{
				"2": {Data: "bbb"},
			},
			mockValues: [][]byte{
				marshalCache("TestGetMulti/mixed/cache,datastore,cache", "1", "aaa"),
				nil,
				marshalCache("TestGetMulti/mixed/cache,datastore,cache", "3", "ccc"),
			},
			keys:    []string{"1", "2", "3"},
			want:    []entity{{Data: "aaa"}, {Data: "bbb"}, {Data: "ccc"}},
			wantSet: []string{"2"},
		},
		{
			name: "mixed/no such entity,datastore,cache",
			datastore: map[string]entity{
				"2": {Data: "bbb"},
			},
			mockValues: [][]byte{
				nil,
				nil,
				marshalCache("TestGetMulti/mixed/no_such_entity,datastore,cache", "3", "ccc"),
			},
			keys:    []string{"1", "2", "3"},
			want:    []entity{{}, {Data: "bbb"}, {Data: "ccc"}},
			wantErr: datastore.MultiError{datastore.ErrNoSuchEntity, nil, nil},
			wantSet: []string{"2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{values: tt.mockValues}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			keys := make([]*datastore.Key, len(tt.keys))
			for i, k := range tt.keys {
				keys[i] = datastore.NameKey(t.Name(), k, nil)
			}
			dst := make([]entity, len(tt.keys))
			err := client.GetMulti(context.Background(), keys, dst)
			if merr, ok := err.(datastore.MultiError); ok {
				if !reflect.DeepEqual(merr, tt.wantErr) {
					t.Fatalf("client.GetMulti() error =  %v, wantErr %v", merr, tt.wantErr)
				}
			} else {
				if err != tt.wantErr {
					t.Fatalf("client.GetMulti() error =  %v, wantErr %v", err, tt.wantErr)
				}
			}
			if !reflect.DeepEqual(dst, tt.want) {
				t.Errorf("client.GetMulti() dst = %v, want %v", dst, tt.want)
			}
			if !reflect.DeepEqual(m.setKeys, tt.wantSet) {
				t.Errorf("called cacher.SetMulti() with keys = %v, want %v", m.setKeys, tt.wantSet)
			}
		})
	}
}

func TestPut(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		key     *datastore.Key
		wantErr error
		wantDel bool
	}{
		{
			name:    "insert",
			key:     datastore.IncompleteKey("TestPut/insert", nil),
			wantDel: false,
		},
		{
			name:    "upsert",
			key:     datastore.NameKey("TestPut/upsert", "1", nil),
			wantDel: true,
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			key:     datastore.NameKey("TestPut/fail", "1", nil),
			wantErr: errDelCache,
			wantDel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			_, err := client.Put(context.Background(), tt.key, &entity{})
			if err != tt.wantErr {
				t.Fatalf("client.Put() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if isDel := len(m.delKeys) > 0; isDel != tt.wantDel {
				t.Errorf("called cacher.DeleteMulti() is %t, want %t", isDel, tt.wantDel)
			}
		})
	}
}

func TestPutMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		keys    []*datastore.Key
		wantErr error
		wantDel []*datastore.Key
	}{
		{
			name: "insert",
			keys: []*datastore.Key{
				datastore.IncompleteKey("TestPutMulti/insert", nil),
				datastore.IncompleteKey("TestPutMulti/insert", nil),
			},
		},
		{
			name: "upsert",
			keys: []*datastore.Key{
				datastore.NameKey("TestPutMulti/upsert", "1", nil),
				datastore.NameKey("TestPutMulti/upsert", "2", nil),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestPutMulti/upsert", "1", nil),
				datastore.NameKey("TestPutMulti/upsert", "2", nil),
			},
		},
		{
			name: "both",
			keys: []*datastore.Key{
				datastore.IncompleteKey("TestPutMulti/both", nil),
				datastore.NameKey("TestPutMulti/both", "upsert", nil),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestPutMulti/both", "upsert", nil),
			},
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			keys: []*datastore.Key{
				datastore.NameKey("TestPutMulti/fail", "1", nil),
				datastore.NameKey("TestPutMulti/fail", "2", nil),
			},
			wantErr: errDelCache,
			wantDel: []*datastore.Key{
				datastore.NameKey("TestPutMulti/fail", "1", nil),
				datastore.NameKey("TestPutMulti/fail", "2", nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			_, err := client.PutMulti(context.Background(), tt.keys, make([]entity, len(tt.keys)))
			if err != tt.wantErr {
				t.Fatalf("client.PutMulti() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, tt.wantDel) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, tt.wantDel)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		key     string
		wantErr error
	}{
		{
			name: "success",
			key:  "ok",
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			key:     "error",
			wantErr: errDelCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			err := client.Delete(context.Background(), datastore.NameKey(t.Name(), tt.key, nil))
			if err != tt.wantErr {
				t.Fatalf("client.Delete() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if len(m.delKeys) != 1 {
				t.Error("cacher.DeleteMulti() is not called")
			}
		})
	}
}

func TestDeleteMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		keys    []string
		wantErr error
	}{
		{
			name: "success",
			keys: []string{"1", "2"},
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			keys:    []string{"1", "2"},
			wantErr: errDelCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			keys := make([]*datastore.Key, len(tt.keys))
			for i, k := range tt.keys {
				keys[i] = datastore.NameKey(t.Name(), k, nil)
			}
			err := client.DeleteMulti(context.Background(), keys)
			if err != tt.wantErr {
				t.Fatalf("client.DeleteMulti() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, keys) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, keys)
			}
		})
	}
}

func TestMutate(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name      string
		datastore map[string]entity
		mockErr   error
		args      []*datastore.Mutation
		wantErr   error
		wantDel   []*datastore.Key
	}{
		{
			name: "insert",
			args: []*datastore.Mutation{
				datastore.NewInsert(datastore.NameKey("TestMutate/insert", "insert", nil), &entity{}),
			},
		},
		{
			name: "update",
			datastore: map[string]entity{
				"update": {Data: "aaa"},
			},
			args: []*datastore.Mutation{
				datastore.NewUpdate(datastore.NameKey("TestMutate/update", "update", nil), &entity{}),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestMutate/update", "update", nil),
			},
		},
		{
			name: "upsert",
			args: []*datastore.Mutation{
				datastore.NewUpsert(datastore.NameKey("TestMutate/upsert", "upsert", nil), &entity{}),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestMutate/upsert", "upsert", nil),
			},
		},
		{
			name: "delete",
			args: []*datastore.Mutation{
				datastore.NewDelete(datastore.NameKey("TestMutate/delete", "delete", nil)),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestMutate/delete", "delete", nil),
			},
		},
		{
			name: "all",
			datastore: map[string]entity{
				"update": {Data: "aaa"},
			},
			args: []*datastore.Mutation{
				datastore.NewInsert(datastore.NameKey("TestMutate/all", "insert", nil), &entity{}),
				datastore.NewUpdate(datastore.NameKey("TestMutate/all", "update", nil), &entity{}),
				datastore.NewUpsert(datastore.NameKey("TestMutate/all", "upsert", nil), &entity{}),
				datastore.NewDelete(datastore.NameKey("TestMutate/all", "delete", nil)),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestMutate/all", "update", nil),
				datastore.NameKey("TestMutate/all", "upsert", nil),
				datastore.NameKey("TestMutate/all", "delete", nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			_, err := client.Mutate(context.Background(), tt.args...)
			if err != tt.wantErr {
				t.Fatalf("client.Mutate() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, tt.wantDel) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, tt.wantDel)
			}
		})
	}
}

func TestTxGet(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name      string
		datastore map[string]entity
		key       string
		want      entity
		wantErr   error
	}{
		{
			name: "success",
			datastore: map[string]entity{
				"ok": {Data: "datastore"},
			},
			key:  "ok",
			want: entity{Data: "datastore"},
		},
		{
			name:    "no such entity",
			key:     "not_exists",
			wantErr: datastore.ErrNoSuchEntity,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{values: [][]byte{marshalCache(t.Name(), tt.key, "not used")}}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			var dst entity
			err = tx.Get(datastore.NameKey(t.Name(), tt.key, nil), &dst)
			if err != tt.wantErr {
				t.Fatalf("tx.Get() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(dst, tt.want) {
				t.Errorf("tx.Get() dst = %v, want %v", dst, tt.want)
			}
			if len(m.setKeys) > 0 {
				t.Error("cacher.SetMulti() is called")
			}
		})
	}
}

func TestTxGetMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name      string
		datastore map[string]entity
		keys      []string
		want      []entity
		wantErr   error
	}{
		{
			name: "success",
			datastore: map[string]entity{
				"1": {Data: "aaa"},
				"2": {Data: "bbb"},
			},
			keys: []string{"1", "2"},
			want: []entity{{Data: "aaa"}, {Data: "bbb"}},
		},
		{
			name:    "no such entity",
			keys:    []string{"1", "2"},
			want:    []entity{{}, {}},
			wantErr: datastore.MultiError{datastore.ErrNoSuchEntity, datastore.ErrNoSuchEntity},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{values: make([][]byte, len(tt.keys))}
			for i, k := range tt.keys {
				m.values[i] = marshalCache(t.Name(), k, "not used")
			}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			keys := make([]*datastore.Key, len(tt.keys))
			for i, k := range tt.keys {
				keys[i] = datastore.NameKey(t.Name(), k, nil)
			}
			dst := make([]entity, len(tt.keys))
			err = tx.GetMulti(keys, dst)
			if merr, ok := err.(datastore.MultiError); ok {
				if !reflect.DeepEqual(merr, tt.wantErr) {
					t.Fatalf("tx.GetMulti() error =  %v, wantErr %v", merr, tt.wantErr)
				}
			} else {
				if err != tt.wantErr {
					t.Fatalf("tx.GetMulti() error =  %v, wantErr %v", err, tt.wantErr)
				}
			}
			if !reflect.DeepEqual(dst, tt.want) {
				t.Errorf("tx.GetMulti() dst = %v, want %v", dst, tt.want)
			}
			if len(m.setKeys) > 0 {
				t.Error("cacher.SetMulti() is called")
			}
		})
	}
}

func TestTxPut(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		key     *datastore.Key
		wantErr error
		wantDel bool
	}{
		{
			name:    "insert",
			key:     datastore.IncompleteKey("TestTxPut/insert", nil),
			wantDel: false,
		},
		{
			name:    "upsert",
			key:     datastore.NameKey("TestTxPut/upsert", "1", nil),
			wantDel: true,
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			key:     datastore.NameKey("TestTxPut/fail", "1", nil),
			wantErr: errDelCache,
			wantDel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Put(tt.key, &entity{}); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Commit()
			if err != tt.wantErr {
				t.Fatalf("tx.Put() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if isDel := len(m.delKeys) > 0; isDel != tt.wantDel {
				t.Errorf("called cacher.DeleteMulti() is %t, want %t", isDel, tt.wantDel)
			}
		})
	}
}

func TestTxPutMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		keys    []*datastore.Key
		wantErr error
		wantDel []*datastore.Key
	}{
		{
			name: "insert",
			keys: []*datastore.Key{
				datastore.IncompleteKey("TestTxPutMulti/insert", nil),
				datastore.IncompleteKey("TestTxPutMulti/insert", nil),
			},
		},
		{
			name: "upsert",
			keys: []*datastore.Key{
				datastore.NameKey("TestTxPutMulti/upsert", "1", nil),
				datastore.NameKey("TestTxPutMulti/upsert", "2", nil),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxPutMulti/upsert", "1", nil),
				datastore.NameKey("TestTxPutMulti/upsert", "2", nil),
			},
		},
		{
			name: "both",
			keys: []*datastore.Key{
				datastore.IncompleteKey("TestTxPutMulti/both", nil),
				datastore.NameKey("TestTxPutMulti/both", "upsert", nil),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxPutMulti/both", "upsert", nil),
			},
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			keys: []*datastore.Key{
				datastore.NameKey("TestTxPutMulti/fail", "1", nil),
				datastore.NameKey("TestTxPutMulti/fail", "2", nil),
			},
			wantErr: errDelCache,
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxPutMulti/fail", "1", nil),
				datastore.NameKey("TestTxPutMulti/fail", "2", nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.PutMulti(tt.keys, make([]entity, len(tt.keys))); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Commit()
			if err != tt.wantErr {
				t.Fatalf("tx.PutMulti() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, tt.wantDel) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, tt.wantDel)
			}
		})
	}
}

func TestTxDelete(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		key     string
		wantErr error
	}{
		{
			name: "success",
			key:  "ok",
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			key:     "error",
			wantErr: errDelCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Delete(datastore.NameKey(t.Name(), tt.key, nil)); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Commit()
			if err != tt.wantErr {
				t.Fatalf("tx.Delete() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if len(m.delKeys) != 1 {
				t.Error("cacher.DeleteMulti() is not called")
			}
		})
	}
}

func TestTxDeleteMulti(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name    string
		mockErr error
		keys    []string
		wantErr error
	}{
		{
			name: "success",
			keys: []string{"1", "2"},
		},
		{
			name:    "fail",
			mockErr: errDelCache,
			keys:    []string{"1", "2"},
			wantErr: errDelCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			keys := make([]*datastore.Key, len(tt.keys))
			for i, k := range tt.keys {
				keys[i] = datastore.NameKey(t.Name(), k, nil)
			}
			if err := tx.DeleteMulti(keys); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Commit()
			if err != tt.wantErr {
				t.Fatalf("tx.DeleteMulti() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, keys) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, keys)
			}
		})
	}
}

func TestTxMutate(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name      string
		datastore map[string]entity
		mockErr   error
		args      []*datastore.Mutation
		wantErr   error
		wantDel   []*datastore.Key
	}{
		{
			name: "insert",
			args: []*datastore.Mutation{
				datastore.NewInsert(datastore.NameKey("TestTxMutate/insert", "insert", nil), &entity{}),
			},
		},
		{
			name: "update",
			datastore: map[string]entity{
				"update": {Data: "aaa"},
			},
			args: []*datastore.Mutation{
				datastore.NewUpdate(datastore.NameKey("TestTxMutate/update", "update", nil), &entity{}),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxMutate/update", "update", nil),
			},
		},
		{
			name: "upsert",
			args: []*datastore.Mutation{
				datastore.NewUpsert(datastore.NameKey("TestTxMutate/upsert", "upsert", nil), &entity{}),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxMutate/upsert", "upsert", nil),
			},
		},
		{
			name: "delete",
			args: []*datastore.Mutation{
				datastore.NewDelete(datastore.NameKey("TestTxMutate/delete", "delete", nil)),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxMutate/delete", "delete", nil),
			},
		},
		{
			name: "all",
			datastore: map[string]entity{
				"update": {Data: "aaa"},
			},
			args: []*datastore.Mutation{
				datastore.NewInsert(datastore.NameKey("TestTxMutate/all", "insert", nil), &entity{}),
				datastore.NewUpdate(datastore.NameKey("TestTxMutate/all", "update", nil), &entity{}),
				datastore.NewUpsert(datastore.NameKey("TestTxMutate/all", "upsert", nil), &entity{}),
				datastore.NewDelete(datastore.NameKey("TestTxMutate/all", "delete", nil)),
			},
			wantDel: []*datastore.Key{
				datastore.NameKey("TestTxMutate/all", "update", nil),
				datastore.NameKey("TestTxMutate/all", "upsert", nil),
				datastore.NameKey("TestTxMutate/all", "delete", nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{err: tt.mockErr}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			tx, err := client.NewTransaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Mutate(tt.args...); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Commit()
			if err != tt.wantErr {
				t.Fatalf("tx.Mutate() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(m.delKeys, tt.wantDel) {
				t.Errorf("called cacher.DeleteMulti() with keys = %v, want %v", m.delKeys, tt.wantDel)
			}
		})
	}
}

func TestRunInTransaction(t *testing.T) {
	defer resetEmulator()

	tests := []struct {
		name          string
		datastore     map[string]entity
		mockErr       error
		key           string
		data          string
		txErr         error
		wantDatastore entity
		wantErr       error
		wantDel       bool
	}{
		{
			name: "success",
			datastore: map[string]entity{
				"ok": {Data: "RunInTransaction: "},
			},
			key:           "ok",
			data:          "done",
			wantDatastore: entity{Data: "RunInTransaction: done"},
			wantDel:       true,
		},
		{
			name: "fail",
			datastore: map[string]entity{
				"fail": {Data: "RunInTransaction: "},
			},
			mockErr:       errDelCache,
			key:           "fail",
			data:          "done",
			wantDatastore: entity{Data: "RunInTransaction: done"},
			wantErr:       errDelCache,
			wantDel:       true,
		},
		{
			name: "rollback",
			datastore: map[string]entity{
				"rollback": {Data: "RunInTransaction: "},
			},
			key:           "rollback",
			data:          "done",
			txErr:         errRunInTransaction,
			wantDatastore: entity{Data: "RunInTransaction: "},
			wantErr:       errRunInTransaction,
			wantDel:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock{
				values: [][]byte{marshalCache(t.Name(), tt.key, "not used")},
				err:    tt.mockErr,
			}
			client := datastoreClientWithInterceptor(t, m)
			defer client.Close()

			putEntities(t, tt.datastore)

			key := datastore.NameKey(t.Name(), tt.key, nil)
			_, err := client.RunInTransaction(context.Background(), func(tx *datastore.Transaction) error {
				var dst entity
				if err := tx.Get(key, &dst); err != nil {
					return fmt.Errorf("get error: %v", err)
				}
				dst.Data += tt.data

				if _, err := tx.Put(key, &dst); err != nil {
					return fmt.Errorf("put error: %v", err)
				}
				return tt.txErr
			})
			if err != tt.wantErr {
				t.Fatalf("client.RunInTransaction() error =  %v, wantErr %v", err, tt.wantErr)
			}
			if isDel := len(m.delKeys) > 0; isDel != tt.wantDel {
				t.Errorf("called cacher.DeleteMulti() is %t, want %t", isDel, tt.wantDel)
			}

			got := getEntity(t, tt.key)
			if !reflect.DeepEqual(got, tt.wantDatastore) {
				t.Errorf("datastore result: %v, want %v", got, tt.wantDatastore)
			}
		})
	}
}

type mock struct {
	values [][]byte
	err    error

	setKeys []string
	delKeys []*datastore.Key
}

func (m *mock) GetMulti(ctx context.Context, keys []*datastorepb.Key) [][]byte {
	return m.values
}

func (m *mock) SetMulti(ctx context.Context, keys []*datastorepb.Key, items [][]byte) {
	for _, k := range keys {
		m.setKeys = append(m.setKeys, k.Path[0].GetName())
	}
}

func (m *mock) DeleteMulti(ctx context.Context, keys []*datastorepb.Key) error {
	for _, k := range keys {
		switch p := k.Path[0].GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			m.delKeys = append(m.delKeys, datastore.IDKey(k.Path[0].Kind, p.Id, nil))
		case *datastorepb.Key_PathElement_Name:
			m.delKeys = append(m.delKeys, datastore.NameKey(k.Path[0].Kind, p.Name, nil))
		}
	}
	return m.err
}

func marshalCache(kind, name string, data string) []byte {
	b, _ := proto.Marshal(&datastorepb.EntityResult{
		Entity: &datastorepb.Entity{
			Key: &datastorepb.Key{
				PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
				Path: []*datastorepb.Key_PathElement{
					{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
				},
			},
			Properties: map[string]*datastorepb.Value{
				"Data": {ValueType: &datastorepb.Value_StringValue{StringValue: data}},
			},
		},
	})
	return b
}

func putEntities(t *testing.T, m map[string]entity) {
	t.Helper()

	if m == nil {
		return
	}

	ctx := context.Background()

	client, err := datastore.NewClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for k, v := range m {
		if _, err := client.Put(ctx, datastore.NameKey(t.Name(), k, nil), &v); err != nil {
			t.Fatal(err)
		}
	}
}

func getEntity(t *testing.T, key string) entity {
	t.Helper()

	ctx := context.Background()

	client, err := datastore.NewClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var v entity
	if err := client.Get(ctx, datastore.NameKey(t.Name(), key, nil), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMain(m *testing.M) {
	gcloud, err := exec.LookPath("gcloud")
	if err != nil {
		log.Fatal(err)
	}

	done, err := startEmulator(gcloud)
	if err != nil {
		log.Fatal(err)
	}

	if err := setEmulatorEnv(gcloud); err != nil {
		done()
		log.Fatal(err)
	}

	code := m.Run()
	done()
	os.Exit(code)
}

var emulator = []string{"beta", "emulators", "datastore"}

func startEmulator(gcloud string) (func(), error) {
	cmd := exec.Command(gcloud, append(emulator, "start", "--project", "test", "--no-store-on-disk", "--consistency", "1.0")...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	ch := make(chan struct{})
	go func() {
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			if s.Text() == "[datastore] Dev App Server is now running." {
				ch <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-ch:
		break
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		return nil, errors.New("timeout starting datastore emulator")
	}

	return func() {
		http.Post(os.Getenv("DATASTORE_HOST")+"/shutdown", "", nil)
	}, nil
}

func setEmulatorEnv(gcloud string) error {
	stdout, err := exec.Command(gcloud, append(emulator, "env-init")...).Output()
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(stdout[:len(stdout)-1]), "\n") {
		env := strings.Split(strings.TrimPrefix(line, "export "), "=")
		if len(env) != 2 && env[0] == "" || env[1] == "" {
			continue
		}
		os.Setenv(env[0], env[1])
	}
	return nil
}

func resetEmulator() {
	http.Post(os.Getenv("DATASTORE_HOST")+"/reset", "", nil)
}
