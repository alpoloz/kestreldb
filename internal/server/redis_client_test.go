package server

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"kestreldb/internal/engine"
)

func TestGoRedisClientCompatibility(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run("RESP"+string(rune('0'+protocol)), func(t *testing.T) {
			db := engine.NewDB()
			h := &handler{db: db}
			var connections sync.WaitGroup
			client := redis.NewClient(&redis.Options{
				Addr:       "kestreldb:0",
				Protocol:   protocol,
				PoolSize:   1,
				MaxRetries: -1,
				Dialer: func(context.Context, string, string) (net.Conn, error) {
					serverConn, clientConn := net.Pipe()
					connections.Add(1)
					go func() {
						defer connections.Done()
						h.serve(serverConn)
					}()
					return clientConn, nil
				},
			})
			defer func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
				connections.Wait()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if pong, err := client.Ping(ctx).Result(); err != nil || pong != "PONG" {
				t.Fatalf("PING = %q, %v", pong, err)
			}

			binaryKey := "binary\x00key"
			binaryValue := "line one\r\nline two\x00\xff"
			if err := client.Set(ctx, binaryKey, binaryValue, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if got, err := client.Get(ctx, binaryKey).Result(); err != nil || got != binaryValue {
				t.Fatalf("binary GET = %q, %v", got, err)
			}

			pipe := client.Pipeline()
			pipe.Set(ctx, "pipeline:one", "1", 0)
			pipe.Set(ctx, "pipeline:two", "2", 0)
			first := pipe.Get(ctx, "pipeline:one")
			second := pipe.Get(ctx, "pipeline:two")
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatal(err)
			}
			if first.Val() != "1" || second.Val() != "2" {
				t.Fatalf("pipeline values = %q, %q", first.Val(), second.Val())
			}

			if added, err := client.HSet(ctx, "hash", "alpha", "one", "beta", "two").Result(); err != nil || added != 2 {
				t.Fatalf("HSET = %d, %v", added, err)
			}
			if got, err := client.HGetAll(ctx, "hash").Result(); err != nil || !reflect.DeepEqual(got, map[string]string{"alpha": "one", "beta": "two"}) {
				t.Fatalf("HGETALL = %v, %v", got, err)
			}

			if _, err := client.RPush(ctx, "list", "a", "b", "c").Result(); err != nil {
				t.Fatal(err)
			}
			if got, err := client.LRange(ctx, "list", 0, -1).Result(); err != nil || !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
				t.Fatalf("LRANGE = %v, %v", got, err)
			}

			if _, err := client.SAdd(ctx, "set", "red", "green").Result(); err != nil {
				t.Fatal(err)
			}
			members, err := client.SMembers(ctx, "set").Result()
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(members)
			if !reflect.DeepEqual(members, []string{"green", "red"}) {
				t.Fatalf("SMEMBERS = %v", members)
			}

			if _, err := client.ZAdd(ctx, "zset", redis.Z{Score: 1.5, Member: "first"}, redis.Z{Score: 2.5, Member: "second"}).Result(); err != nil {
				t.Fatal(err)
			}
			scores, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			if err != nil || len(scores) != 2 || scores[0].Member != "first" || scores[0].Score != 1.5 || scores[1].Member != "second" || scores[1].Score != 2.5 {
				t.Fatalf("ZRANGE WITHSCORES = %#v, %v", scores, err)
			}

			if err := client.Set(ctx, "wrongtype", "value", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.HGet(ctx, "wrongtype", "field").Err(); err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE ") {
				t.Fatalf("wrong-type error = %v", err)
			}
			if _, err := client.Get(ctx, "missing").Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("missing GET error = %v", err)
			}
		})
	}
}
