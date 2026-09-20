package server

import (
	"strconv"
	"strings"
)

// Command metadata is shared by read-only replica enforcement and key routing.
// Syntax-specific arity and option checks remain with each handler.
type commandMeta struct {
	readOnly bool
	keySpec  keySpec
}

type keySpec uint8

const (
	noKeys keySpec = iota
	firstKey
	allKeys
	pairKeys
	firstTwoKeys
	allButLastKey
	storeKeys
	numKeys
	storeNumKeys
)

var commands = makeCommandMetadata()

func makeCommandMetadata() map[string]commandMeta {
	m := make(map[string]commandMeta)
	add := func(readOnly bool, spec keySpec, names string) {
		for _, name := range strings.Fields(names) {
			m[name] = commandMeta{readOnly: readOnly, keySpec: spec}
		}
	}
	add(true, noKeys, "PING QUIT HELLO ECHO SELECT CLIENT INFO")
	add(true, firstKey, "TYPE TTL PTTL EXPIRETIME PEXPIRETIME GET STRLEN GETRANGE LLEN LRANGE LINDEX HGET HMGET HLEN HEXISTS HKEYS HVALS HSTRLEN HTTL HPTTL HEXPIRETIME HPEXPIRETIME HGETALL HSCAN SISMEMBER SMISMEMBER SCARD SMEMBERS SRANDMEMBER SSCAN ZSCORE ZMSCORE ZRANK ZREVRANK ZCARD ZCOUNT ZLEXCOUNT ZRANGE ZRANDMEMBER ZSCAN ZRANGEWITHSCORES")
	add(true, allKeys, "EXISTS MGET SUNION SINTER SDIFF")
	add(true, numKeys, "ZUNION ZINTER ZDIFF ZINTERCARD")
	add(false, firstKey, "EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST SET SETNX GETSET GETDEL APPEND SETRANGE INCR DECR INCRBY DECRBY INCRBYFLOAT LPUSH RPUSH LPOP RPOP LSET LTRIM LINSERT LREM HSET HSETNX HDEL HEXPIRE HPEXPIRE HEXPIREAT HPEXPIREAT HPERSIST HINCRBY HINCRBYFLOAT SADD SADDEX SREM SPOP ZADD ZREM ZINCRBY ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX ZPOPMIN ZPOPMAX")
	add(false, allKeys, "DEL")
	add(false, pairKeys, "MSET MSETNX")
	add(false, firstTwoKeys, "SMOVE LMOVE RPOPLPUSH BLMOVE BRPOPLPUSH ZRANGESTORE")
	add(false, allButLastKey, "BLPOP BRPOP")
	add(false, storeKeys, "SUNIONSTORE SINTERSTORE SDIFFSTORE")
	add(false, numKeys, "ZMPOP")
	add(false, storeNumKeys, "ZUNIONSTORE ZINTERSTORE ZDIFFSTORE")
	return m
}

func commandKeys(name string, args []string) []string {
	meta, ok := commands[name]
	if !ok || len(args) == 0 {
		return nil
	}
	switch meta.keySpec {
	case firstKey:
		return args[:1]
	case allKeys:
		return args
	case pairKeys:
		keys := make([]string, 0, (len(args)+1)/2)
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		return keys
	case firstTwoKeys:
		if len(args) < 2 {
			return args
		}
		return args[:2]
	case allButLastKey:
		return args[:len(args)-1]
	case storeKeys:
		return args
	case numKeys, storeNumKeys:
		start := 0
		keys := []string(nil)
		if meta.keySpec == storeNumKeys {
			keys = append(keys, args[0])
			start = 1
		}
		if len(args) <= start {
			return keys
		}
		n, err := strconv.Atoi(args[start])
		if err != nil || n < 0 || len(args) < start+1+n {
			return keys
		}
		return append(keys, args[start+1:start+1+n]...)
	default:
		return nil
	}
}
