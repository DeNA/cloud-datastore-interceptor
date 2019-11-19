# cloud-datastore-interceptor

[![GoDoc](https://godoc.org/github.com/DeNA/cloud-datastore-interceptor?status.svg)](https://godoc.org/github.com/DeNA/cloud-datastore-interceptor)

Package `github.com/DeNA/cloud-datastore-interceptor` provides gRPC interceptors for Cloud Datastore([cloud.google.com/go/datastore](https://godoc.org/cloud.google.com/go/datastore)).

## Examples

### In-Memory cache

This will use in-memory cache on [Client.Get](https://godoc.org/cloud.google.com/go/datastore#Client.Get) and [Client.GetMulti](https://godoc.org/cloud.google.com/go/datastore#Client.GetMulti).

```go
opts := []option.ClientOption{
		option.WithGRPCDialOption(
			grpc.WithUnaryInterceptor(
				cache.UnaryClientInterceptor(memory.NewCache(1 * time.Minute)),
			),
		),
}
client, err := datastore.NewClient(ctx, projID, opts...)
```

### Cache query results

[cache.UnaryClientInterceptor](https://godoc.org/github.com/DeNA/cloud-datastore-interceptor/cache#UnaryClientInterceptor) does not use the cache for [Query](https://godoc.org/cloud.google.com/go/datastore#Query)(e.g., [Client.GetAll](https://godoc.org/cloud.google.com/go/datastore#Client.GetAll), [Client.Run](https://godoc.org/cloud.google.com/go/datastore#Client.Run)), but by using [transform.QueryToLookupWithKeysOnly](https://godoc.org/github.com/DeNA/cloud-datastore-interceptor/transform#QueryToLookupWithKeysOnly), it is transformed to gRPC equivalent to [Client.GetMulti](https://godoc.org/cloud.google.com/go/datastore#Client.GetMulti) and the cache is used.

```go
opts := []option.ClientOption{
		option.WithGRPCDialOption(
			grpc.WithChainUnaryInterceptor(
				transform.QueryToLookupWithKeysOnly(),
				cache.UnaryClientInterceptor(memory.NewCache(1 * time.Minute)),
			),
		),
}
client, err := datastore.NewClient(ctx, projID, opts...)
```

### Redis cache

Same as [In-Memory cache](#in-memory-cache), but the backend is Redis using [redisClient](https://godoc.org/github.com/go-redis/redis#Client).

```go
opts := []option.ClientOption{
		option.WithGRPCDialOption(
			grpc.WithUnaryInterceptor(
				cache.UnaryClientInterceptor(redis.NewCache(1 * time.Minute, redisClient)),
			),
		),
}
client, err := datastore.NewClient(ctx, projID, opts...)
```
