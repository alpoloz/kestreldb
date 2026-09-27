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

			streamID, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "events", ID: "1-0", Values: map[string]any{"type": "created"}}).Result()
			if err != nil || streamID != "1-0" {
				t.Fatalf("XADD = %q, %v", streamID, err)
			}
			streamEntries, err := client.XRange(ctx, "events", "-", "+").Result()
			if err != nil || len(streamEntries) != 1 || streamEntries[0].ID != "1-0" || streamEntries[0].Values["type"] != "created" {
				t.Fatalf("XRANGE = %#v, %v", streamEntries, err)
			}
			if err := client.XGroupCreate(ctx, "events", "workers", "0-0").Err(); err != nil {
				t.Fatal(err)
			}
			groupEntries, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "workers", Consumer: "alice", Streams: []string{"events", ">"}, Count: 1}).Result()
			if err != nil || len(groupEntries) != 1 || len(groupEntries[0].Messages) != 1 || groupEntries[0].Messages[0].ID != "1-0" {
				t.Fatalf("XREADGROUP = %#v, %v", groupEntries, err)
			}
			pending, err := client.XPending(ctx, "events", "workers").Result()
			if err != nil || pending.Count != 1 || pending.Consumers["alice"] != 1 {
				t.Fatalf("XPENDING = %#v, %v", pending, err)
			}
			claimed, err := client.XClaim(ctx, &redis.XClaimArgs{Stream: "events", Group: "workers", Consumer: "bob", MinIdle: 0, Messages: []string{"1-0"}}).Result()
			if err != nil || len(claimed) != 1 || claimed[0].ID != "1-0" {
				t.Fatalf("XCLAIM = %#v, %v", claimed, err)
			}
			if acked, err := client.XAck(ctx, "events", "workers", "1-0").Result(); err != nil || acked != 1 {
				t.Fatalf("XACK = %d, %v", acked, err)
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
