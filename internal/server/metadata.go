package server

import (
	"fmt"
	"strconv"
	"strings"
)

// Command metadata is the option-independent source of truth for arity,
// behavioral flags, and key routing. Handlers retain validation for option
// combinations and arguments whose shape depends on earlier values.
type commandMeta struct {
	flags   commandFlags
	keySpec keySpec
	minArgs int
	maxArgs int // -1 means unbounded.
	shape   argumentShape
}

type commandFlags uint8

const (
	commandReadOnly commandFlags = 1 << iota
	commandBlocking
	commandAdmin
)

func (m commandMeta) has(flag commandFlags) bool { return m.flags&flag != 0 }

type argumentShape uint8

const (
	anyArguments argumentShape = iota
	evenArguments
	keyAndPairs
)

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
	add := func(flags commandFlags, spec keySpec, minArgs, maxArgs int, names string) {
		for _, name := range strings.Fields(names) {
			m[name] = commandMeta{flags: flags, keySpec: spec, minArgs: minArgs, maxArgs: maxArgs}
		}
	}
	addShape := func(flags commandFlags, spec keySpec, minArgs, maxArgs int, shape argumentShape, names string) {
		add(flags, spec, minArgs, maxArgs, names)
		for _, name := range strings.Fields(names) {
			meta := m[name]
			meta.shape = shape
			m[name] = meta
		}
	}

	const unlimited = -1
	const read = commandReadOnly

	add(read, noKeys, 0, 1, "INFO")
	add(read, noKeys, 1, 1, "HELLO ECHO SELECT")
	add(read, noKeys, 3, 3, "CLIENT")
	add(read, noKeys, 0, 0, "PING QUIT DBSIZE")
	add(read|commandAdmin, noKeys, 1, unlimited, "CLUSTER")
	add(read, noKeys, 1, 1, "KEYS")
	add(read, noKeys, 1, unlimited, "SCAN")

	add(read, firstKey, 1, 1, "TYPE TTL PTTL EXPIRETIME PEXPIRETIME GET STRLEN HLEN HKEYS HVALS HGETALL LLEN SCARD SMEMBERS ZCARD")
	add(read, firstKey, 2, 2, "HGET HEXISTS HSTRLEN SISMEMBER ZSCORE ZRANK ZREVRANK LINDEX")
	add(read, firstKey, 2, unlimited, "HMGET SMISMEMBER HSCAN SSCAN ZSCAN ZMSCORE")
	add(read, firstKey, 3, 3, "GETRANGE LRANGE ZCOUNT ZLEXCOUNT ZRANGEWITHSCORES")
	add(read, firstKey, 3, unlimited, "ZRANGE")
	add(read, firstKey, 1, 2, "SRANDMEMBER")
	add(read, firstKey, 1, 3, "ZRANDMEMBER")
	add(read, allKeys, 1, unlimited, "EXISTS TOUCH MGET SUNION SINTER SDIFF")
	add(read, numKeys, 2, unlimited, "ZUNION ZINTER ZDIFF ZINTERCARD")

	add(0, noKeys, 0, 1, "FLUSHDB")
	add(0, allKeys, 1, unlimited, "DEL")
	add(0, firstTwoKeys, 2, 2, "RENAME RENAMENX RPOPLPUSH")
	add(0, firstTwoKeys, 2, 5, "COPY")
	add(0, firstTwoKeys, 3, 3, "SMOVE BRPOPLPUSH")
	add(0, firstTwoKeys, 4, 4, "LMOVE")
	add(commandBlocking, firstTwoKeys, 5, 5, "BLMOVE")
	add(commandBlocking, allButLastKey, 2, unlimited, "BLPOP BRPOP")

	add(0, firstKey, 1, 1, "PERSIST GETDEL INCR DECR")
	add(0, firstKey, 1, 2, "LPOP RPOP SPOP ZPOPMIN ZPOPMAX")
	add(0, firstKey, 2, 2, "SETNX GETSET APPEND INCRBY DECRBY INCRBYFLOAT")
	add(0, firstKey, 2, unlimited, "LPUSH RPUSH HDEL SADD SREM ZREM SET")
	add(0, firstKey, 3, 3, "SETRANGE LSET LTRIM LREM HSETNX HINCRBY HINCRBYFLOAT ZINCRBY ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX")
	add(0, firstKey, 4, 4, "LINSERT")
	add(0, firstKey, 3, unlimited, "SADDEX ZADD")
	add(0, firstKey, 2, 3, "EXPIRE PEXPIRE EXPIREAT PEXPIREAT")
	add(0, firstKey, 4, unlimited, "HEXPIRE HPEXPIRE HEXPIREAT HPEXPIREAT")
	add(0, firstKey, 3, unlimited, "HTTL HPTTL HEXPIRETIME HPEXPIRETIME HPERSIST")
	addShape(0, pairKeys, 2, unlimited, evenArguments, "MSET MSETNX")
	addShape(0, firstKey, 3, unlimited, keyAndPairs, "HSET")

	add(0, storeKeys, 2, unlimited, "SUNIONSTORE SINTERSTORE SDIFFSTORE")
	add(0, firstTwoKeys, 4, unlimited, "ZRANGESTORE")
	add(0, numKeys, 3, unlimited, "ZMPOP")
	add(0, storeNumKeys, 3, unlimited, "ZUNIONSTORE ZINTERSTORE ZDIFFSTORE")

	return m
}

func validateCommandMetadata(name string, args []string) error {
	meta, ok := commands[name]
	if !ok {
		return nil
	}
	if len(args) < meta.minArgs || meta.maxArgs >= 0 && len(args) > meta.maxArgs {
		return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(name))
	}
	switch meta.shape {
	case evenArguments:
		if len(args)%2 != 0 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(name))
		}
	case keyAndPairs:
		if len(args)%2 == 0 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(name))
		}
	}
	return nil
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
		keys := make([]string, 0, len(args)/2)
		for i := 0; i+1 < len(args); i += 2 {
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
