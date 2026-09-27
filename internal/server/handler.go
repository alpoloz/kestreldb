package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

type handler struct {
	db     *engine.DB
	server *Server
}

var errReadOnly = errors.New("You can't write against a read only replica")

type request struct {
	tokens []string
	mode   proto.Mode
}

func (h *handler) serve(conn net.Conn) {
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := bufio.NewReader(conn)
	w := proto.NewWriter(bufio.NewWriter(conn))
	var first request
	for {
		tokens, mode, err := proto.ReadRequest(r)
		if errors.Is(err, proto.ErrEmptyCommand) {
			continue
		}
		if err != nil {
			return
		}
		first = request{tokens: tokens, mode: mode}
		break
	}
	if h.server != nil && strings.EqualFold(first.tokens[0], "REPLSYNC") {
		h.server.serveReplica(conn, r, first.tokens)
		return
	}
	commands := make(chan request, 1)
	commands <- first
	go readCommands(ctx, cancel, r, commands)

	for {
		var command request
		select {
		case <-ctx.Done():
			return
		case command = <-commands:
		}
		if command.mode == proto.ModeKLP {
			w.SetMode(proto.ModeKLP)
		} else if w.Mode() == proto.ModeKLP {
			w.SetMode(proto.ModeRESP2)
		}

		quit, dispatchErr := h.dispatchContext(ctx, w, command.tokens)
		if ctx.Err() != nil {
			return
		}
		if dispatchErr != nil {
			_ = writeDispatchError(w, dispatchErr)
		}
		_ = w.Flush()

		if quit {
			return
		}
	}
}

func (h *handler) dispatch(w *proto.Writer, tokens []string) (quit bool, err error) {
	return h.dispatchContext(context.Background(), w, tokens)
}

func (h *handler) dispatchContext(
	ctx context.Context,
	w *proto.Writer,
	tokens []string,
) (quit bool, err error) {
	if len(tokens) == 0 {
		return false, proto.ErrEmptyCommand
	}
	cmd := strings.ToUpper(tokens[0])
	args := tokens[1:]
	if err := validateCommandMetadata(cmd, args); err != nil {
		return false, err
	}
	if h.server != nil {
		if err := h.server.routeCommand(cmd, args); err != nil {
			return false, err
		}
	}
	if h.server != nil && h.server.isReplica() {
		if meta, ok := commands[cmd]; ok && !meta.has(commandReadOnly) {
			return false, errReadOnly
		}
	}

	switch cmd {
	case "INFO":
		if len(args) > 1 || len(args) == 1 && !strings.EqualFold(args[0], "replication") {
			return false, errors.New("unsupported INFO section")
		}
		if h.server == nil {
			return false, w.WriteBlobString([]byte("role:master\r\n"))
		}
		return false, w.WriteBlobString([]byte(h.server.replicationInfo()))
	case "HELLO":
		if len(args) != 1 || (args[0] != "2" && args[0] != "3") {
			return false, errors.New("unsupported protocol version")
		}
		if args[0] == "2" {
			w.SetMode(proto.ModeRESP2)
		} else {
			w.SetMode(proto.ModeRESP3)
		}
		if err := w.WriteMapHeader(3); err != nil {
			return false, err
		}
		for _, field := range [][2]string{{"server", "kestreldb"}, {"version", "0.1"}, {"proto", args[0]}} {
			if err := w.WriteBlobString([]byte(field[0])); err != nil {
				return false, err
			}
			if err := w.WriteBlobString([]byte(field[1])); err != nil {
				return false, err
			}
		}
		return false, nil
	case "ECHO":
		return false, w.WriteBlobString([]byte(args[0]))
	case "SELECT":
		if args[0] != "0" {
			return false, errors.New("only database 0 is supported")
		}
		return false, w.WriteSimpleString("OK")
	case "CLIENT":
		if len(args) >= 1 && strings.EqualFold(args[0], "SETINFO") && len(args) == 3 {
			return false, w.WriteSimpleString("OK")
		}
		return false, errors.New("unsupported CLIENT subcommand")
	case "CLUSTER":
		if h.server == nil {
			return false, errors.New("cluster mode is disabled")
		}
		return false, h.server.writeClusterCommand(w, args)
	case "PING":
		return false, w.WriteSimpleString("PONG")

	case "QUIT":
		_ = w.WriteSimpleString("BYE")
		return true, nil

	// ── Generic key commands ─────────────────────────────────────────────────

	case "TYPE":
		return false, w.WriteSimpleString(h.db.Type(args[0]).String())

	case "DEL":
		return false, w.WriteInt(h.db.Del(args...))

	case "EXISTS":
		return false, w.WriteInt(h.db.Exists(args...))

	case "RENAME", "RENAMENX":
		renamed, err := h.db.Rename(args[0], args[1], cmd == "RENAMENX")
		if err != nil {
			return false, err
		}
		if cmd == "RENAMENX" {
			return false, w.WriteInt(boolInt(renamed))
		}
		return false, w.WriteSimpleString("OK")

	case "COPY":
		replace, err := parseCopyOptions(args[2:])
		if err != nil {
			return false, err
		}
		copied, err := h.db.Copy(args[0], args[1], replace)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(boolInt(copied))

	case "TOUCH":
		return false, w.WriteInt(h.db.Touch(args...))

	case "DBSIZE":
		return false, w.WriteInt(h.db.DBSize())

	case "FLUSHDB":
		if len(args) > 1 || len(args) == 1 && !strings.EqualFold(args[0], "SYNC") && !strings.EqualFold(args[0], "ASYNC") {
			return false, errors.New("syntax error")
		}
		if len(args) == 1 && strings.EqualFold(args[0], "ASYNC") {
			return false, errors.New("asynchronous FLUSHDB is not supported")
		}
		h.db.FlushDB()
		return false, w.WriteSimpleString("OK")

	case "KEYS":
		return false, writeBlobArray(w, h.db.Keys(args[0]))

	case "SCAN":
		cursor, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		options, err := parseScanOptions(args[1:], true)
		if err != nil {
			return false, err
		}
		next, keys := h.db.Scan(cursor, options)
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(strconv.FormatUint(next, 10))); err != nil {
			return false, err
		}
		return false, writeBlobArray(w, keys)

	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		value, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		deadline, err := expirationDeadline(cmd, value, h.db.NowUnixMilli())
		if err != nil {
			return false, err
		}
		options, err := parseExpireOptions(args[2:])
		if err != nil {
			return false, err
		}
		changed, err := h.db.ExpireAt(args[0], time.UnixMilli(deadline), options)
		if err != nil {
			return false, err
		}
		if changed {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "TTL", "PTTL":
		return false, w.WriteInt64(h.db.TTL(args[0], cmd == "PTTL"))

	case "EXPIRETIME", "PEXPIRETIME":
		return false, w.WriteInt64(h.db.ExpireTime(args[0], cmd == "PEXPIRETIME"))

	case "PERSIST":
		if h.db.Persist(args[0]) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	// ── String ────────────────────────────────────────────────────────────────

	case "SET":
		options, err := parseSetOptions(args[2:], h.db.NowUnixMilli())
		if err != nil {
			return false, err
		}
		result, err := h.db.SetWithOptions(args[0], []byte(args[1]), options)
		if err != nil {
			return false, err
		}
		if options.Get {
			if !result.PreviousFound {
				return false, w.WriteNil()
			}
			return false, w.WriteBlobString(result.Previous)
		}
		if !result.Stored {
			return false, w.WriteNil()
		}
		return false, w.WriteSimpleString("OK")

	case "SETNX":
		if h.db.SetNX(args[0], []byte(args[1])) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "GETSET":
		value, found, err := h.db.GetSet(args[0], []byte(args[1]))
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "GETDEL":
		value, found, err := h.db.GetDel(args[0])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "GET":
		value, found, err := h.db.Get(args[0])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "MSET":
		pairs := make([]engine.StringPair, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			pairs = append(pairs, engine.StringPair{Key: args[i], Value: []byte(args[i+1])})
		}
		h.db.MSet(pairs...)
		return false, w.WriteSimpleString("OK")

	case "MSETNX":
		pairs := make([]engine.StringPair, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			pairs = append(pairs, engine.StringPair{Key: args[i], Value: []byte(args[i+1])})
		}
		if h.db.MSetNX(pairs...) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "MGET":
		results := h.db.MGet(args...)
		if err := w.WriteArrayHeader(len(results)); err != nil {
			return false, err
		}
		for _, result := range results {
			if !result.Found {
				if err := w.WriteNil(); err != nil {
					return false, err
				}
				continue
			}
			if err := w.WriteBlobString(result.Value); err != nil {
				return false, err
			}
		}
		return false, nil

	case "APPEND":
		n, err := h.db.Append(args[0], []byte(args[1]))
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "STRLEN":
		n, err := h.db.StrLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "GETRANGE":
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		value, err := h.db.GetRange(args[0], start, stop)
		if err != nil {
			return false, err
		}
		return false, w.WriteBlobString(value)

	case "SETRANGE":
		offset, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		length, err := h.db.SetRange(args[0], offset, []byte(args[2]))
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "INCR", "DECR":
		var result int64
		var err error
		if cmd == "INCR" {
			result, err = h.db.IncrBy(args[0], 1)
		} else {
			result, err = h.db.DecrBy(args[0], 1)
		}
		if err != nil {
			return false, err
		}
		return false, w.WriteInt64(result)

	case "INCRBY", "DECRBY":
		operand, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		var result int64
		if cmd == "INCRBY" {
			result, err = h.db.IncrBy(args[0], operand)
		} else {
			result, err = h.db.DecrBy(args[0], operand)
		}
		if err != nil {
			return false, err
		}
		return false, w.WriteInt64(result)

	case "INCRBYFLOAT":
		increment, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return false, engine.ErrInvalidFloat
		}
		result, err := h.db.IncrByFloat(args[0], increment)
		if err != nil {
			return false, err
		}
		return false, w.WriteBlobString([]byte(result))

	// ── List ──────────────────────────────────────────────────────────────────

	case "LPUSH", "RPUSH":
		var length int
		var err error
		if cmd == "LPUSH" {
			length, err = h.db.LPush(args[0], args[1:]...)
		} else {
			length, err = h.db.RPush(args[0], args[1:]...)
		}
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "LPOP", "RPOP":
		count := int64(1)
		withCount := len(args) == 2
		if withCount {
			var err error
			count, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil || count < 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		var values []string
		var err error
		if cmd == "LPOP" {
			values, err = h.db.LPop(args[0], count)
		} else {
			values, err = h.db.RPop(args[0], count)
		}
		if err != nil {
			return false, err
		}
		if withCount {
			return false, writeBlobArray(w, values)
		}
		if len(values) == 0 {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(values[0]))

	case "LLEN":
		length, err := h.db.LLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "LRANGE":
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		values, err := h.db.LRange(args[0], start, stop)
		if err != nil {
			return false, err
		}
		return false, writeBlobArray(w, values)

	case "LINDEX":
		index, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		value, found, err := h.db.LIndex(args[0], index)
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "LSET":
		index, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		if err := h.db.LSet(args[0], index, args[2]); err != nil {
			return false, err
		}
		return false, w.WriteSimpleString("OK")

	case "LTRIM":
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		if err := h.db.LTrim(args[0], start, stop); err != nil {
			return false, err
		}
		return false, w.WriteSimpleString("OK")

	case "LINSERT":
		var before bool
		switch strings.ToUpper(args[1]) {
		case "BEFORE":
			before = true
		case "AFTER":
			before = false
		default:
			return false, errors.New("syntax error")
		}
		length, err := h.db.LInsert(args[0], before, args[2], args[3])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "LREM":
		count, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		removed, err := h.db.LRem(args[0], count, args[2])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "LMOVE":
		from, err := parseListDirection(args[2])
		if err != nil {
			return false, err
		}
		to, err := parseListDirection(args[3])
		if err != nil {
			return false, err
		}
		value, found, err := h.db.LMove(args[0], args[1], from, to)
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "RPOPLPUSH":
		value, found, err := h.db.RPopLPush(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "BLPOP", "BRPOP":
		waitCtx, cancel, err := listWaitContext(ctx, args[len(args)-1])
		if err != nil {
			return false, err
		}
		defer cancel()
		direction := engine.ListLeft
		if cmd == "BRPOP" {
			direction = engine.ListRight
		}
		key, value, found, err := h.db.WaitForListPop(waitCtx, args[:len(args)-1], direction)
		if errors.Is(err, context.DeadlineExceeded) {
			return false, w.WriteNullArray()
		}
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNullArray()
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(key)); err != nil {
			return false, err
		}
		return false, w.WriteBlobString([]byte(value))

	case "BLMOVE":
		from, err := parseListDirection(args[2])
		if err != nil {
			return false, err
		}
		to, err := parseListDirection(args[3])
		if err != nil {
			return false, err
		}
		waitCtx, cancel, err := listWaitContext(ctx, args[4])
		if err != nil {
			return false, err
		}
		defer cancel()
		value, found, err := h.db.WaitForListMove(waitCtx, args[0], args[1], from, to)
		if errors.Is(err, context.DeadlineExceeded) {
			return false, w.WriteNil()
		}
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "BRPOPLPUSH":
		waitCtx, cancel, err := listWaitContext(ctx, args[2])
		if err != nil {
			return false, err
		}
		defer cancel()
		value, found, err := h.db.WaitForListMove(
			waitCtx, args[0], args[1], engine.ListRight, engine.ListLeft,
		)
		if errors.Is(err, context.DeadlineExceeded) {
			return false, w.WriteNil()
		}
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	// ── Hash ──────────────────────────────────────────────────────────────────

	case "HSET":
		pairs := make([]engine.HashPair, 0, (len(args)-1)/2)
		for index := 1; index < len(args); index += 2 {
			pairs = append(pairs, engine.HashPair{Field: args[index], Value: args[index+1]})
		}
		n, err := h.db.HSetMany(args[0], pairs...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "HSETNX":
		stored, err := h.db.HSetNX(args[0], args[1], args[2])
		if err != nil {
			return false, err
		}
		if stored {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "HGET":
		value, found, err := h.db.HGet(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "HMGET":
		results, err := h.db.HMGet(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(results)); err != nil {
			return false, err
		}
		for _, result := range results {
			if !result.Found {
				if err := w.WriteNil(); err != nil {
					return false, err
				}
				continue
			}
			if err := w.WriteBlobString([]byte(result.Value)); err != nil {
				return false, err
			}
		}
		return false, nil

	case "HDEL":
		n, err := h.db.HDel(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "HLEN":
		n, err := h.db.HLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "HEXISTS":
		found, err := h.db.HExists(args[0], args[1])
		if err != nil {
			return false, err
		}
		if found {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "HKEYS", "HVALS":
		var values []string
		var err error
		if cmd == "HKEYS" {
			values, err = h.db.HKeys(args[0])
		} else {
			values, err = h.db.HVals(args[0])
		}
		if err != nil {
			return false, err
		}
		return false, writeBlobArray(w, values)

	case "HSTRLEN":
		length, err := h.db.HStrLen(args[0], args[1])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT":
		value, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		deadline, err := expirationDeadline(strings.TrimPrefix(cmd, "H"), value, h.db.NowUnixMilli())
		if err != nil {
			return false, err
		}
		options, fields, err := parseHashExpirationFields(args[2:], true)
		if err != nil {
			return false, err
		}
		results, err := h.db.HExpireAt(args[0], fields, time.UnixMilli(deadline), options)
		if err != nil {
			return false, err
		}
		return false, writeIntArray(w, results)

	case "HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME":
		_, fields, err := parseHashExpirationFields(args[1:], false)
		if err != nil {
			return false, err
		}
		milliseconds := cmd == "HPTTL" || cmd == "HPEXPIRETIME"
		absolute := cmd == "HEXPIRETIME" || cmd == "HPEXPIRETIME"
		results, err := h.db.HFieldTTL(args[0], fields, milliseconds, absolute)
		if err != nil {
			return false, err
		}
		return false, writeInt64Array(w, results)

	case "HPERSIST":
		_, fields, err := parseHashExpirationFields(args[1:], false)
		if err != nil {
			return false, err
		}
		results, err := h.db.HPersist(args[0], fields)
		if err != nil {
			return false, err
		}
		return false, writeIntArray(w, results)

	case "HINCRBY":
		increment, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		result, err := h.db.HIncrBy(args[0], args[1], increment)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt64(result)

	case "HINCRBYFLOAT":
		increment, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return false, engine.ErrInvalidFloat
		}
		result, err := h.db.HIncrByFloat(args[0], args[1], increment)
		if err != nil {
			return false, err
		}
		return false, w.WriteBlobString([]byte(result))

	case "HGETALL":
		pairs, err := h.db.HGetAll(args[0])
		if err != nil {
			return false, err
		}
		if err := w.WriteMapHeader(len(pairs)); err != nil {
			return false, err
		}
		for field, value := range pairs {
			if err := w.WriteBlobString([]byte(field)); err != nil {
				return false, err
			}
			if err := w.WriteBlobString([]byte(value)); err != nil {
				return false, err
			}
		}
		return false, nil

	case "HSCAN":
		cursor, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		options, err := parseScanOptions(args[2:], false)
		if err != nil {
			return false, err
		}
		next, pairs, err := h.db.HScan(args[0], cursor, options)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(strconv.FormatUint(next, 10))); err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(pairs) * 2); err != nil {
			return false, err
		}
		for _, pair := range pairs {
			if err := w.WriteBlobString([]byte(pair.Field)); err != nil {
				return false, err
			}
			if err := w.WriteBlobString([]byte(pair.Value)); err != nil {
				return false, err
			}
		}
		return false, nil

	// ── Set ───────────────────────────────────────────────────────────────────

	case "SADD":
		n, err := h.db.SAdd(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SADDEX":
		seconds, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || seconds <= 0 || seconds > int64(math.MaxInt64/time.Second) {
			return false, engine.ErrInvalidInteger
		}
		n, err := h.db.SAddEx(args[0], time.Duration(seconds)*time.Second, args[2:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SREM":
		n, err := h.db.SRem(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SISMEMBER":
		found, err := h.db.SIsMember(args[0], args[1])
		if err != nil {
			return false, err
		}
		if found {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "SMISMEMBER":
		results, err := h.db.SMIsMember(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(results)); err != nil {
			return false, err
		}
		for _, found := range results {
			value := 0
			if found {
				value = 1
			}
			if err := w.WriteInt(value); err != nil {
				return false, err
			}
		}
		return false, nil

	case "SMOVE":
		moved, err := h.db.SMove(args[0], args[1], args[2])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(moved)

	case "SCARD":
		n, err := h.db.SCard(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SMEMBERS":
		members, err := h.db.SMembers(args[0])
		if err != nil {
			return false, err
		}
		return false, writeBlobSet(w, members)

	case "SPOP":
		count := int64(1)
		withCount := len(args) == 2
		if withCount {
			var err error
			count, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil || count < 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		members, err := h.db.SPop(args[0], count)
		if err != nil {
			return false, err
		}
		if withCount {
			return false, writeBlobArray(w, members)
		}
		if len(members) == 0 {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(members[0]))

	case "SRANDMEMBER":
		count := int64(1)
		withCount := len(args) == 2
		if withCount {
			var err error
			count, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return false, engine.ErrInvalidInteger
			}
		}
		members, err := h.db.SRandMembers(args[0], count)
		if err != nil {
			return false, err
		}
		if withCount {
			return false, writeBlobArray(w, members)
		}
		if len(members) == 0 {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(members[0]))

	case "SSCAN":
		cursor, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		options, err := parseScanOptions(args[2:], false)
		if err != nil {
			return false, err
		}
		next, members, err := h.db.SScan(args[0], cursor, options)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(strconv.FormatUint(next, 10))); err != nil {
			return false, err
		}
		return false, writeBlobArray(w, members)

	case "SUNION", "SINTER", "SDIFF":
		var members []string
		var err error
		switch cmd {
		case "SUNION":
			members, err = h.db.SUnion(args...)
		case "SINTER":
			members, err = h.db.SInter(args...)
		case "SDIFF":
			members, err = h.db.SDiff(args...)
		}
		if err != nil {
			return false, err
		}
		return false, writeBlobSet(w, members)

	case "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE":
		var count int
		var err error
		switch cmd {
		case "SUNIONSTORE":
			count, err = h.db.SUnionStore(args[0], args[1:]...)
		case "SINTERSTORE":
			count, err = h.db.SInterStore(args[0], args[1:]...)
		case "SDIFFSTORE":
			count, err = h.db.SDiffStore(args[0], args[1:]...)
		}
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	// ── Sorted set ────────────────────────────────────────────────────────────

	case "ZADD":
		options, items, err := parseZAddArguments(args[1:])
		if err != nil {
			return false, err
		}
		result, err := h.db.ZAddMany(args[0], options, items...)
		if err != nil {
			return false, err
		}
		if options.INCR {
			if !result.ScoreFound {
				return false, w.WriteNil()
			}
			return false, w.WriteFloat(result.Score)
		}
		return false, w.WriteInt(result.Count)

	case "ZREM":
		n, err := h.db.ZRem(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "ZSCORE":
		score, found, err := h.db.ZScore(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteFloat(score)

	case "ZMSCORE":
		items, found, err := h.db.ZMScore(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(items)); err != nil {
			return false, err
		}
		for index, item := range items {
			if !found[index] {
				if err := w.WriteNil(); err != nil {
					return false, err
				}
				continue
			}
			if err := w.WriteFloat(item.Score); err != nil {
				return false, err
			}
		}
		return false, nil

	case "ZRANK", "ZREVRANK":
		var rank int
		var found bool
		var err error
		if cmd == "ZRANK" {
			rank, found, err = h.db.ZRank(args[0], args[1])
		} else {
			rank, found, err = h.db.ZRevRank(args[0], args[1])
		}
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteInt(rank)

	case "ZCARD":
		// ZCARD key
		n, err := h.db.ZCard(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "ZCOUNT":
		min, err := parseScoreBound(args[1])
		if err != nil {
			return false, err
		}
		max, err := parseScoreBound(args[2])
		if err != nil {
			return false, err
		}
		count, err := h.db.ZCount(args[0], min, max)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZLEXCOUNT":
		min, err := parseLexBound(args[1])
		if err != nil {
			return false, err
		}
		max, err := parseLexBound(args[2])
		if err != nil {
			return false, err
		}
		count, err := h.db.ZLexCount(args[0], min, max)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZRANGE":
		items, withScores, err := executeZRange(h.db, args[0], args[1], args[2], args[3:])
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, withScores)

	case "ZRANGESTORE":
		query, withScores, err := parseZRangeQuery(args[2], args[3], args[4:])
		if err != nil {
			return false, err
		}
		if withScores {
			return false, errors.New("syntax error")
		}
		count, err := h.db.ZRangeStoreFrom(args[0], args[1], query)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZINCRBY":
		increment, err := strconv.ParseFloat(args[1], 64)
		if err != nil || math.IsNaN(increment) {
			return false, engine.ErrInvalidFloat
		}
		score, err := h.db.ZIncrBy(args[0], args[2], increment)
		if err != nil {
			return false, err
		}
		return false, w.WriteFloat(score)

	case "ZREMRANGEBYRANK":
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		removed, err := h.db.ZRemRangeByRank(args[0], start, stop)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "ZREMRANGEBYSCORE":
		min, err := parseScoreBound(args[1])
		if err != nil {
			return false, err
		}
		max, err := parseScoreBound(args[2])
		if err != nil {
			return false, err
		}
		removed, err := h.db.ZRemRangeByScore(args[0], min, max)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "ZREMRANGEBYLEX":
		min, err := parseLexBound(args[1])
		if err != nil {
			return false, err
		}
		max, err := parseLexBound(args[2])
		if err != nil {
			return false, err
		}
		removed, err := h.db.ZRemRangeByLex(args[0], min, max)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "ZPOPMIN", "ZPOPMAX":
		count := int64(1)
		if len(args) == 2 {
			var err error
			count, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil || count < 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		items, err := h.db.ZPop(args[0], cmd == "ZPOPMAX", count)
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, true)

	case "ZMPOP":
		numKeys, err := strconv.Atoi(args[0])
		if err != nil || numKeys <= 0 || len(args) < numKeys+2 {
			return false, engine.ErrInvalidInteger
		}
		maximum := strings.EqualFold(args[numKeys+1], "MAX")
		if !maximum && !strings.EqualFold(args[numKeys+1], "MIN") {
			return false, errors.New("syntax error")
		}
		count := int64(1)
		rest := args[numKeys+2:]
		if len(rest) > 0 {
			if len(rest) != 2 || !strings.EqualFold(rest[0], "COUNT") {
				return false, errors.New("syntax error")
			}
			count, err = strconv.ParseInt(rest[1], 10, 64)
			if err != nil || count <= 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		key, items, found, err := h.db.ZMPop(args[1:numKeys+1], maximum, count)
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNullArray()
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(key)); err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(items)); err != nil {
			return false, err
		}
		for _, item := range items {
			if err := w.WriteArrayHeader(2); err != nil {
				return false, err
			}
			if err := w.WriteBlobString([]byte(item.Member)); err != nil {
				return false, err
			}
			if err := w.WriteFloat(item.Score); err != nil {
				return false, err
			}
		}
		return false, nil

	case "ZRANDMEMBER":
		count := int64(1)
		withCount := len(args) >= 2
		withScores := false
		distinct := true
		if withCount {
			var err error
			count, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return false, engine.ErrInvalidInteger
			}
			if count < 0 {
				if count == math.MinInt64 {
					return false, engine.ErrInvalidInteger
				}
				count = -count
				distinct = false
			}
		}
		if len(args) == 3 {
			if !strings.EqualFold(args[2], "WITHSCORES") {
				return false, errors.New("syntax error")
			}
			withScores = true
		}
		items, err := h.db.ZRandMember(args[0], count, distinct)
		if err != nil {
			return false, err
		}
		if !withCount {
			if len(items) == 0 {
				return false, w.WriteNil()
			}
			return false, w.WriteBlobString([]byte(items[0].Member))
		}
		return false, writeZSetItems(w, items, withScores)

	case "ZSCAN":
		cursor, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		options, err := parseScanOptions(args[2:], false)
		if err != nil {
			return false, err
		}
		next, items, err := h.db.ZScan(args[0], cursor, options)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(strconv.FormatUint(next, 10))); err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, true)

	case "ZUNION", "ZINTER":
		sources, aggregate, withScores, err := parseZCombineArguments(args)
		if err != nil {
			return false, err
		}
		var items []engine.ZSetItem
		if cmd == "ZUNION" {
			items, err = h.db.ZUnion(sources, aggregate)
		} else {
			items, err = h.db.ZInter(sources, aggregate)
		}
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, withScores)

	case "ZDIFF":
		keys, withScores, err := parseZDiffArguments(args)
		if err != nil {
			return false, err
		}
		items, err := h.db.ZDiff(keys...)
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, withScores)

	case "ZUNIONSTORE", "ZINTERSTORE":
		sources, aggregate, withScores, err := parseZCombineArguments(args[1:])
		if err != nil {
			return false, err
		}
		if withScores {
			return false, errors.New("syntax error")
		}
		var count int
		if cmd == "ZUNIONSTORE" {
			count, err = h.db.ZUnionStore(args[0], sources, aggregate)
		} else {
			count, err = h.db.ZInterStore(args[0], sources, aggregate)
		}
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZDIFFSTORE":
		keys, withScores, err := parseZDiffArguments(args[1:])
		if err != nil {
			return false, err
		}
		if withScores {
			return false, errors.New("syntax error")
		}
		count, err := h.db.ZDiffStore(args[0], keys...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZINTERCARD":
		numKeys, err := strconv.Atoi(args[0])
		if err != nil || numKeys <= 0 || len(args) < numKeys+1 {
			return false, engine.ErrInvalidInteger
		}
		limit := int64(0)
		rest := args[numKeys+1:]
		if len(rest) > 0 {
			if len(rest) != 2 || !strings.EqualFold(rest[0], "LIMIT") {
				return false, errors.New("syntax error")
			}
			limit, err = strconv.ParseInt(rest[1], 10, 64)
			if err != nil || limit < 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		count, err := h.db.ZInterCard(args[1:numKeys+1], limit)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "ZRANGEWITHSCORES":
		start, stop, err := parseRange(args[1], args[2])
		if err != nil {
			return false, err
		}
		items, err := h.db.ZRange(args[0], start, stop)
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, true)

	// ── Stream commands ───────────────────────────────────────────────

	case "XADD":
		request, fields, options, err := parseXAddArguments(args[1:])
		if err != nil {
			return false, err
		}
		id, added, err := h.db.XAdd(args[0], request, fields, options)
		if err != nil {
			return false, err
		}
		if !added {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(id.String()))

	case "XLEN":
		length, err := h.db.XLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "XRANGE", "XREVRANGE":
		reverse := cmd == "XREVRANGE"
		first, second := args[1], args[2]
		if reverse {
			first, second = second, first
		}
		start, err := parseStreamRangeBound(first, false)
		if err != nil {
			return false, err
		}
		end, err := parseStreamRangeBound(second, true)
		if err != nil {
			return false, err
		}
		count := int64(0)
		if len(args) > 3 {
			if len(args) != 5 || !strings.EqualFold(args[3], "COUNT") {
				return false, errors.New("syntax error")
			}
			count, err = strconv.ParseInt(args[4], 10, 64)
			if err != nil || count <= 0 {
				return false, engine.ErrInvalidInteger
			}
		}
		entries, err := h.db.XRange(args[0], start, end, count, reverse)
		if err != nil {
			return false, err
		}
		return false, writeStreamEntries(w, entries)

	case "XDEL":
		ids, err := parseStreamIDs(args[1:])
		if err != nil {
			return false, err
		}
		removed, err := h.db.XDel(args[0], ids...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "XTRIM":
		trim, consumed, err := parseStreamTrim(args[1:])
		if err != nil || consumed != len(args)-1 {
			if err != nil {
				return false, err
			}
			return false, errors.New("syntax error")
		}
		removed, err := h.db.XTrim(args[0], trim)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(removed)

	case "XREAD":
		requests, count, block, timeout, err := parseXReadArguments(args)
		if err != nil {
			return false, err
		}
		var results []engine.StreamReadResult
		if block {
			waitCtx, cancel, waitErr := streamWaitContext(ctx, timeout)
			if waitErr != nil {
				return false, waitErr
			}
			defer cancel()
			results, err = h.db.WaitForStreamRead(waitCtx, requests, count)
			if errors.Is(err, context.DeadlineExceeded) {
				return false, w.WriteNullArray()
			}
		} else {
			results, err = h.db.XRead(requests, count)
		}
		if err != nil {
			return false, err
		}
		if len(results) == 0 {
			return false, w.WriteNullArray()
		}
		return false, writeStreamReadResults(w, results)

	case "XGROUP":
		return false, h.writeXGroup(w, args)

	case "XREADGROUP":
		group, consumer, requests, options, block, timeout, err := parseXReadGroupArguments(args)
		if err != nil {
			return false, err
		}
		var results []engine.StreamReadResult
		canBlock := block
		for _, request := range requests {
			canBlock = canBlock && request.NewOnly
		}
		if canBlock {
			waitCtx, cancel, waitErr := streamWaitContext(ctx, timeout)
			if waitErr != nil {
				return false, waitErr
			}
			defer cancel()
			results, err = h.db.WaitForStreamGroupRead(waitCtx, group, consumer, requests, options)
			if errors.Is(err, context.DeadlineExceeded) {
				return false, w.WriteNullArray()
			}
		} else {
			results, err = h.db.XReadGroup(group, consumer, requests, options)
		}
		if err != nil {
			return false, err
		}
		if len(results) == 0 {
			return false, w.WriteNullArray()
		}
		return false, writeStreamReadResults(w, results)

	case "XACK":
		ids, err := parseStreamIDs(args[2:])
		if err != nil {
			return false, err
		}
		count, err := h.db.XAck(args[0], args[1], ids...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(count)

	case "XPENDING":
		return false, h.writeXPending(w, args)

	case "XCLAIM":
		entries, ids, justID, err := h.executeXClaim(args)
		if err != nil {
			return false, err
		}
		if justID {
			return false, writeStreamIDs(w, ids)
		}
		return false, writeStreamEntries(w, entries)

	case "XAUTOCLAIM":
		result, justID, err := h.executeXAutoClaim(args)
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(3); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(result.Next.String())); err != nil {
			return false, err
		}
		if justID {
			if err := writeStreamIDs(w, result.IDs); err != nil {
				return false, err
			}
		} else if err := writeStreamEntries(w, result.Entries); err != nil {
			return false, err
		}
		return false, writeStreamIDs(w, result.Deleted)

	default:
		return false, fmt.Errorf("unknown command %q", cmd)
	}
}

func readCommands(
	ctx context.Context,
	cancel context.CancelFunc,
	r *bufio.Reader,
	commands chan<- request,
) {
	for {
		tokens, mode, err := proto.ReadRequest(r)
		if errors.Is(err, proto.ErrEmptyCommand) {
			continue
		}
		if err != nil {
			cancel()
			return
		}
		select {
		case commands <- request{tokens: tokens, mode: mode}:
		case <-ctx.Done():
			return
		}
	}
}

func parseXAddArguments(args []string) (engine.StreamIDRequest, []engine.StreamField, engine.StreamAddOptions, error) {
	options := engine.StreamAddOptions{}
	index := 0
	for index < len(args) {
		switch strings.ToUpper(args[index]) {
		case "NOMKSTREAM":
			if options.NoMkStream {
				return engine.StreamIDRequest{}, nil, options, errors.New("syntax error")
			}
			options.NoMkStream = true
			index++
		case "MAXLEN", "MINID":
			if options.Trim != nil {
				return engine.StreamIDRequest{}, nil, options, errors.New("syntax error")
			}
			trim, consumed, err := parseStreamTrim(args[index:])
			if err != nil {
				return engine.StreamIDRequest{}, nil, options, err
			}
			options.Trim = &trim
			index += consumed
		default:
			goto parsedOptions
		}
	}

parsedOptions:
	if index >= len(args) {
		return engine.StreamIDRequest{}, nil, options, errors.New("syntax error")
	}
	request, err := parseStreamIDRequest(args[index])
	if err != nil {
		return engine.StreamIDRequest{}, nil, options, err
	}
	index++
	if index >= len(args) || (len(args)-index)%2 != 0 {
		return engine.StreamIDRequest{}, nil, options, errors.New("wrong number of field-value arguments")
	}
	fields := make([]engine.StreamField, 0, (len(args)-index)/2)
	for ; index < len(args); index += 2 {
		fields = append(fields, engine.StreamField{Name: args[index], Value: args[index+1]})
	}
	return request, fields, options, nil
}

func parseStreamIDRequest(value string) (engine.StreamIDRequest, error) {
	if value == "*" {
		return engine.StreamIDRequest{Auto: true}, nil
	}
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return engine.StreamIDRequest{}, engine.ErrInvalidStreamID
	}
	milliseconds, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return engine.StreamIDRequest{}, engine.ErrInvalidStreamID
	}
	if parts[1] == "*" {
		return engine.StreamIDRequest{Milliseconds: milliseconds, AutoSequence: true}, nil
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return engine.StreamIDRequest{}, engine.ErrInvalidStreamID
	}
	return engine.StreamIDRequest{Milliseconds: milliseconds, Sequence: sequence}, nil
}

func parseStreamID(value string) (engine.StreamID, error) {
	parts := strings.Split(value, "-")
	if len(parts) == 1 {
		milliseconds, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return engine.StreamID{}, engine.ErrInvalidStreamID
		}
		return engine.StreamID{Milliseconds: milliseconds}, nil
	}
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return engine.StreamID{}, engine.ErrInvalidStreamID
	}
	milliseconds, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return engine.StreamID{}, engine.ErrInvalidStreamID
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return engine.StreamID{}, engine.ErrInvalidStreamID
	}
	return engine.StreamID{Milliseconds: milliseconds, Sequence: sequence}, nil
}

func parseStreamIDs(values []string) ([]engine.StreamID, error) {
	ids := make([]engine.StreamID, 0, len(values))
	for _, value := range values {
		id, err := parseStreamID(value)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func parseStreamRangeBound(value string, end bool) (engine.StreamRangeBound, error) {
	bound := engine.StreamRangeBound{}
	if value == "-" {
		bound.Unbounded = -1
		return bound, nil
	}
	if value == "+" {
		bound.Unbounded = 1
		return bound, nil
	}
	if strings.HasPrefix(value, "(") {
		bound.Exclusive = true
		value = value[1:]
	}
	parts := strings.Split(value, "-")
	if len(parts) == 1 {
		milliseconds, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return engine.StreamRangeBound{}, engine.ErrInvalidStreamID
		}
		bound.ID.Milliseconds = milliseconds
		if end {
			bound.ID.Sequence = math.MaxUint64
		}
		return bound, nil
	}
	id, err := parseStreamID(value)
	if err != nil {
		return engine.StreamRangeBound{}, err
	}
	bound.ID = id
	return bound, nil
}

func parseStreamTrim(args []string) (engine.StreamTrimOptions, int, error) {
	if len(args) < 2 {
		return engine.StreamTrimOptions{}, 0, errors.New("syntax error")
	}
	options := engine.StreamTrimOptions{}
	switch strings.ToUpper(args[0]) {
	case "MAXLEN":
		options.Mode = engine.StreamTrimMaxLen
	case "MINID":
		options.Mode = engine.StreamTrimMinID
	default:
		return options, 0, errors.New("syntax error")
	}
	index := 1
	if index < len(args) && (args[index] == "=" || args[index] == "~") {
		options.Approximate = args[index] == "~"
		index++
	}
	if index >= len(args) {
		return options, 0, errors.New("syntax error")
	}
	if options.Mode == engine.StreamTrimMaxLen {
		value, err := strconv.ParseInt(args[index], 10, 64)
		if err != nil || value < 0 {
			return options, 0, engine.ErrInvalidInteger
		}
		options.MaxLen = value
	} else {
		id, err := parseStreamID(args[index])
		if err != nil {
			return options, 0, err
		}
		options.MinID = id
	}
	index++
	if index < len(args) && strings.EqualFold(args[index], "LIMIT") {
		if !options.Approximate {
			return options, 0, errors.New("syntax error")
		}
		if index+1 >= len(args) {
			return options, 0, errors.New("syntax error")
		}
		limit, err := strconv.ParseInt(args[index+1], 10, 64)
		if err != nil || limit < 0 {
			return options, 0, engine.ErrInvalidInteger
		}
		options.Limit = limit
		index += 2
	}
	return options, index, nil
}

func parseXReadArguments(args []string) ([]engine.StreamReadRequest, int64, bool, int64, error) {
	count := int64(0)
	block := false
	timeout := int64(0)
	index := 0
	for index < len(args) && !strings.EqualFold(args[index], "STREAMS") {
		if index+1 >= len(args) {
			return nil, 0, false, 0, errors.New("syntax error")
		}
		switch strings.ToUpper(args[index]) {
		case "COUNT":
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value <= 0 {
				return nil, 0, false, 0, engine.ErrInvalidInteger
			}
			count = value
		case "BLOCK":
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value < 0 {
				return nil, 0, false, 0, engine.ErrInvalidInteger
			}
			block, timeout = true, value
		default:
			return nil, 0, false, 0, errors.New("syntax error")
		}
		index += 2
	}
	if index >= len(args) || !strings.EqualFold(args[index], "STREAMS") {
		return nil, 0, false, 0, errors.New("syntax error")
	}
	requests, err := parseStreamReadRequests(args[index+1:], false)
	return requests, count, block, timeout, err
}

func parseStreamReadRequests(args []string, group bool) ([]engine.StreamReadRequest, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("Unbalanced XREAD list of streams")
	}
	half := len(args) / 2
	requests := make([]engine.StreamReadRequest, half)
	for index := 0; index < half; index++ {
		request := engine.StreamReadRequest{Key: args[index]}
		selector := args[half+index]
		if group && selector == ">" {
			request.NewOnly = true
		} else if !group && selector == "$" {
			request.UseLatest = true
		} else {
			id, err := parseStreamID(selector)
			if err != nil {
				return nil, err
			}
			request.After = id
		}
		requests[index] = request
	}
	return requests, nil
}

func streamWaitContext(parent context.Context, milliseconds int64) (context.Context, context.CancelFunc, error) {
	if milliseconds == 0 {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, nil
	}
	if milliseconds > math.MaxInt64/int64(time.Millisecond) {
		return nil, nil, errors.New("timeout is out of range")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(milliseconds)*time.Millisecond)
	return ctx, cancel, nil
}

func (h *handler) writeXGroup(w *proto.Writer, args []string) error {
	switch strings.ToUpper(args[0]) {
	case "CREATE":
		if len(args) != 4 && len(args) != 5 || len(args) == 5 && !strings.EqualFold(args[4], "MKSTREAM") {
			return errors.New("syntax error")
		}
		start := engine.StreamID{}
		useLast := args[3] == "$"
		if !useLast {
			var err error
			start, err = parseStreamID(args[3])
			if err != nil {
				return err
			}
		}
		if err := h.db.XGroupCreate(args[1], args[2], start, useLast, len(args) == 5); err != nil {
			return err
		}
		return w.WriteSimpleString("OK")
	case "DESTROY":
		if len(args) != 3 {
			return errors.New("syntax error")
		}
		removed, err := h.db.XGroupDestroy(args[1], args[2])
		if err != nil {
			return err
		}
		return w.WriteInt(boolInt(removed))
	case "SETID":
		if len(args) != 4 {
			return errors.New("syntax error")
		}
		id := engine.StreamID{}
		useLast := args[3] == "$"
		if !useLast {
			var err error
			id, err = parseStreamID(args[3])
			if err != nil {
				return err
			}
		}
		if err := h.db.XGroupSetID(args[1], args[2], id, useLast); err != nil {
			return err
		}
		return w.WriteSimpleString("OK")
	case "CREATECONSUMER":
		if len(args) != 4 {
			return errors.New("syntax error")
		}
		created, err := h.db.XGroupCreateConsumer(args[1], args[2], args[3])
		if err != nil {
			return err
		}
		return w.WriteInt(boolInt(created))
	case "DELCONSUMER":
		if len(args) != 4 {
			return errors.New("syntax error")
		}
		removed, err := h.db.XGroupDelConsumer(args[1], args[2], args[3])
		if err != nil {
			return err
		}
		return w.WriteInt(removed)
	default:
		return errors.New("unknown XGROUP subcommand")
	}
}

func parseXReadGroupArguments(args []string) (string, string, []engine.StreamReadRequest, engine.StreamGroupReadOptions, bool, int64, error) {
	if len(args) < 4 || !strings.EqualFold(args[0], "GROUP") {
		return "", "", nil, engine.StreamGroupReadOptions{}, false, 0, errors.New("syntax error")
	}
	group, consumer := args[1], args[2]
	options := engine.StreamGroupReadOptions{}
	block := false
	timeout := int64(0)
	index := 3
	for index < len(args) && !strings.EqualFold(args[index], "STREAMS") {
		switch strings.ToUpper(args[index]) {
		case "COUNT":
			if index+1 >= len(args) {
				return "", "", nil, options, false, 0, errors.New("syntax error")
			}
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value <= 0 {
				return "", "", nil, options, false, 0, engine.ErrInvalidInteger
			}
			options.Count = value
			index += 2
		case "BLOCK":
			if index+1 >= len(args) {
				return "", "", nil, options, false, 0, errors.New("syntax error")
			}
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value < 0 {
				return "", "", nil, options, false, 0, engine.ErrInvalidInteger
			}
			block, timeout = true, value
			index += 2
		case "NOACK":
			options.NoAck = true
			index++
		default:
			return "", "", nil, options, false, 0, errors.New("syntax error")
		}
	}
	if index >= len(args) || !strings.EqualFold(args[index], "STREAMS") {
		return "", "", nil, options, false, 0, errors.New("syntax error")
	}
	requests, err := parseStreamReadRequests(args[index+1:], true)
	return group, consumer, requests, options, block, timeout, err
}

func (h *handler) writeXPending(w *proto.Writer, args []string) error {
	if len(args) == 2 {
		summary, err := h.db.XPendingSummary(args[0], args[1])
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(4); err != nil {
			return err
		}
		if err := w.WriteInt(summary.Count); err != nil {
			return err
		}
		if summary.Count == 0 {
			if err := w.WriteNil(); err != nil {
				return err
			}
			if err := w.WriteNil(); err != nil {
				return err
			}
		} else {
			if err := w.WriteBlobString([]byte(summary.Smallest.String())); err != nil {
				return err
			}
			if err := w.WriteBlobString([]byte(summary.Greatest.String())); err != nil {
				return err
			}
		}
		names := make([]string, 0, len(summary.Consumers))
		for name := range summary.Consumers {
			names = append(names, name)
		}
		sort.Strings(names)
		if err := w.WriteArrayHeader(len(names)); err != nil {
			return err
		}
		for _, name := range names {
			if err := w.WriteArrayHeader(2); err != nil {
				return err
			}
			if err := w.WriteBlobString([]byte(name)); err != nil {
				return err
			}
			if err := w.WriteInt(summary.Consumers[name]); err != nil {
				return err
			}
		}
		return nil
	}
	if len(args) != 5 && len(args) != 6 {
		return errors.New("syntax error")
	}
	start, err := parseStreamRangeBound(args[2], false)
	if err != nil {
		return err
	}
	end, err := parseStreamRangeBound(args[3], true)
	if err != nil {
		return err
	}
	count, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil || count <= 0 {
		return engine.ErrInvalidInteger
	}
	consumer := ""
	if len(args) == 6 {
		consumer = args[5]
	}
	items, err := h.db.XPendingRange(args[0], args[1], start, end, count, consumer)
	if err != nil {
		return err
	}
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.WriteArrayHeader(4); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(item.ID.String())); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(item.Consumer)); err != nil {
			return err
		}
		if err := w.WriteInt64(item.IdleMillis); err != nil {
			return err
		}
		if err := w.WriteInt64(int64(item.Deliveries)); err != nil {
			return err
		}
	}
	return nil
}

func (h *handler) executeXClaim(args []string) ([]engine.StreamEntry, []engine.StreamID, bool, error) {
	minIdle, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil || minIdle < 0 {
		return nil, nil, false, engine.ErrInvalidInteger
	}
	index := 4
	for index < len(args) && !isXClaimOption(args[index]) {
		index++
	}
	if index == 4 {
		return nil, nil, false, errors.New("syntax error")
	}
	ids, err := parseStreamIDs(args[4:index])
	if err != nil {
		return nil, nil, false, err
	}
	options := engine.StreamClaimOptions{}
	for index < len(args) {
		switch strings.ToUpper(args[index]) {
		case "IDLE":
			if index+1 >= len(args) {
				return nil, nil, false, errors.New("syntax error")
			}
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value < 0 {
				return nil, nil, false, engine.ErrInvalidInteger
			}
			options.IdleMillis = &value
			index += 2
		case "TIME":
			if index+1 >= len(args) {
				return nil, nil, false, errors.New("syntax error")
			}
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value < 0 {
				return nil, nil, false, engine.ErrInvalidInteger
			}
			options.TimeMillis = &value
			index += 2
		case "RETRYCOUNT":
			if index+1 >= len(args) {
				return nil, nil, false, errors.New("syntax error")
			}
			value, err := strconv.ParseUint(args[index+1], 10, 64)
			if err != nil {
				return nil, nil, false, engine.ErrInvalidInteger
			}
			options.RetryCount = &value
			index += 2
		case "FORCE":
			options.Force = true
			index++
		case "JUSTID":
			options.JustID = true
			index++
		default:
			return nil, nil, false, errors.New("syntax error")
		}
	}
	entries, claimed, err := h.db.XClaim(args[0], args[1], args[2], minIdle, ids, options)
	return entries, claimed, options.JustID, err
}

func isXClaimOption(value string) bool {
	switch strings.ToUpper(value) {
	case "IDLE", "TIME", "RETRYCOUNT", "FORCE", "JUSTID":
		return true
	default:
		return false
	}
}

func (h *handler) executeXAutoClaim(args []string) (engine.StreamAutoClaimResult, bool, error) {
	minIdle, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil || minIdle < 0 {
		return engine.StreamAutoClaimResult{}, false, engine.ErrInvalidInteger
	}
	start, err := parseStreamID(args[4])
	if err != nil {
		return engine.StreamAutoClaimResult{}, false, err
	}
	count := int64(100)
	justID := false
	for index := 5; index < len(args); {
		switch strings.ToUpper(args[index]) {
		case "COUNT":
			if index+1 >= len(args) {
				return engine.StreamAutoClaimResult{}, false, errors.New("syntax error")
			}
			count, err = strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || count <= 0 {
				return engine.StreamAutoClaimResult{}, false, engine.ErrInvalidInteger
			}
			index += 2
		case "JUSTID":
			justID = true
			index++
		default:
			return engine.StreamAutoClaimResult{}, false, errors.New("syntax error")
		}
	}
	result, err := h.db.XAutoClaim(args[0], args[1], args[2], minIdle, start, count, justID)
	return result, justID, err
}

func writeStreamEntries(w *proto.Writer, entries []engine.StreamEntry) error {
	if err := w.WriteArrayHeader(len(entries)); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := w.WriteArrayHeader(2); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(entry.ID.String())); err != nil {
			return err
		}
		if err := w.WriteArrayHeader(len(entry.Fields) * 2); err != nil {
			return err
		}
		for _, field := range entry.Fields {
			if err := w.WriteBlobString([]byte(field.Name)); err != nil {
				return err
			}
			if err := w.WriteBlobString([]byte(field.Value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeStreamReadResults(w *proto.Writer, results []engine.StreamReadResult) error {
	if err := w.WriteArrayHeader(len(results)); err != nil {
		return err
	}
	for _, result := range results {
		if err := w.WriteArrayHeader(2); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(result.Key)); err != nil {
			return err
		}
		if err := writeStreamEntries(w, result.Entries); err != nil {
			return err
		}
	}
	return nil
}

func writeStreamIDs(w *proto.Writer, ids []engine.StreamID) error {
	if err := w.WriteArrayHeader(len(ids)); err != nil {
		return err
	}
	for _, id := range ids {
		if err := w.WriteBlobString([]byte(id.String())); err != nil {
			return err
		}
	}
	return nil
}

func parseListDirection(value string) (engine.ListDirection, error) {
	switch strings.ToUpper(value) {
	case "LEFT":
		return engine.ListLeft, nil
	case "RIGHT":
		return engine.ListRight, nil
	default:
		return 0, errors.New("syntax error")
	}
}

func listWaitContext(parent context.Context, timeoutArg string) (context.Context, context.CancelFunc, error) {
	seconds, err := strconv.ParseFloat(timeoutArg, 64)
	if err != nil || seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return nil, nil, errors.New("timeout is not a valid number")
	}
	if seconds == 0 {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, nil
	}
	maxSeconds := float64(math.MaxInt64) / float64(time.Second)
	if seconds > maxSeconds {
		return nil, nil, errors.New("timeout is out of range")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(seconds*float64(time.Second)))
	return ctx, cancel, nil
}

func writeBlobArray(w *proto.Writer, values []string) error {
	if err := w.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := w.WriteBlobString([]byte(value)); err != nil {
			return err
		}
	}
	return nil
}

func writeBlobSet(w *proto.Writer, values []string) error {
	if err := w.WriteSetHeader(len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := w.WriteBlobString([]byte(value)); err != nil {
			return err
		}
	}
	return nil
}

func writeIntArray(w *proto.Writer, values []int) error {
	if err := w.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := w.WriteInt(value); err != nil {
			return err
		}
	}
	return nil
}

func writeInt64Array(w *proto.Writer, values []int64) error {
	if err := w.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := w.WriteInt64(value); err != nil {
			return err
		}
	}
	return nil
}

func parseScanOptions(args []string, allowType bool) (engine.ScanOptions, error) {
	options := engine.ScanOptions{}
	for index := 0; index < len(args); index += 2 {
		if index+1 >= len(args) {
			return engine.ScanOptions{}, errors.New("syntax error")
		}
		switch strings.ToUpper(args[index]) {
		case "MATCH":
			options.Match = args[index+1]
			options.UseMatch = true
		case "COUNT":
			count, err := strconv.Atoi(args[index+1])
			if err != nil || count <= 0 {
				return engine.ScanOptions{}, engine.ErrInvalidInteger
			}
			options.Count = count
		case "TYPE":
			if !allowType {
				return engine.ScanOptions{}, errors.New("syntax error")
			}
			options.Type = strings.ToLower(args[index+1])
			options.UseType = true
		default:
			return engine.ScanOptions{}, errors.New("syntax error")
		}
	}
	return options, nil
}

func parseCopyOptions(args []string) (bool, error) {
	replace := false
	replaceSeen := false
	databaseSeen := false
	for index := 0; index < len(args); index++ {
		switch strings.ToUpper(args[index]) {
		case "REPLACE":
			if replaceSeen {
				return false, errors.New("syntax error")
			}
			replace = true
			replaceSeen = true
		case "DB":
			if databaseSeen || index+1 >= len(args) {
				return false, errors.New("syntax error")
			}
			database, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil {
				return false, engine.ErrInvalidInteger
			}
			if database != 0 {
				return false, errors.New("only database 0 is supported")
			}
			databaseSeen = true
			index++
		default:
			return false, errors.New("syntax error")
		}
	}
	return replace, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func parseSetOptions(args []string, nowMillis int64) (engine.SetOptions, error) {
	options := engine.SetOptions{}
	expirationSeen := false
	for index := 0; index < len(args); index++ {
		switch strings.ToUpper(args[index]) {
		case "NX":
			if options.NX || options.XX {
				return engine.SetOptions{}, errors.New("syntax error")
			}
			options.NX = true
		case "XX":
			if options.NX || options.XX {
				return engine.SetOptions{}, errors.New("syntax error")
			}
			options.XX = true
		case "GET":
			if options.Get {
				return engine.SetOptions{}, errors.New("syntax error")
			}
			options.Get = true
		case "KEEPTTL":
			if options.KeepTTL || expirationSeen {
				return engine.SetOptions{}, errors.New("syntax error")
			}
			options.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if expirationSeen || options.KeepTTL || index+1 >= len(args) {
				return engine.SetOptions{}, errors.New("syntax error")
			}
			value, err := strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || value <= 0 {
				return engine.SetOptions{}, errors.New("invalid expire time in SET")
			}
			deadline, err := expirationDeadline(strings.ToUpper(args[index]), value, nowMillis)
			if err != nil {
				return engine.SetOptions{}, err
			}
			options.HasExpiration = true
			options.ExpireAt = deadline
			expirationSeen = true
			index++
		default:
			return engine.SetOptions{}, errors.New("syntax error")
		}
	}
	return options, nil
}

func parseExpireOptions(args []string) (engine.ExpireOptions, error) {
	if len(args) == 0 {
		return engine.ExpireOptions{}, nil
	}
	if len(args) != 1 {
		return engine.ExpireOptions{}, errors.New("syntax error")
	}
	options := engine.ExpireOptions{}
	switch strings.ToUpper(args[0]) {
	case "NX":
		options.NX = true
	case "XX":
		options.XX = true
	case "GT":
		options.GT = true
	case "LT":
		options.LT = true
	default:
		return engine.ExpireOptions{}, errors.New("syntax error")
	}
	return options, nil
}

func parseHashExpirationFields(args []string, allowOption bool) (engine.ExpireOptions, []string, error) {
	options := engine.ExpireOptions{}
	if allowOption && len(args) > 0 && !strings.EqualFold(args[0], "FIELDS") {
		var err error
		options, err = parseExpireOptions(args[:1])
		if err != nil {
			return engine.ExpireOptions{}, nil, err
		}
		args = args[1:]
	}
	if len(args) < 3 || !strings.EqualFold(args[0], "FIELDS") {
		return engine.ExpireOptions{}, nil, errors.New("syntax error")
	}
	count, err := strconv.Atoi(args[1])
	if err != nil || count <= 0 {
		return engine.ExpireOptions{}, nil, engine.ErrInvalidInteger
	}
	if len(args) != count+2 {
		return engine.ExpireOptions{}, nil, errors.New("syntax error")
	}
	return options, args[2:], nil
}

func expirationDeadline(command string, value, nowMillis int64) (int64, error) {
	if value <= 0 {
		return 0, nil
	}
	absolute := command == "EXAT" || command == "PXAT" || command == "EXPIREAT" || command == "PEXPIREAT"
	seconds := command == "EX" || command == "EXAT" || command == "EXPIRE" || command == "EXPIREAT"
	deadline := value
	if seconds {
		if value > math.MaxInt64/1000 {
			return 0, engine.ErrInvalidInteger
		}
		deadline = value * 1000
	}
	if absolute {
		return deadline, nil
	}
	if deadline > math.MaxInt64-nowMillis {
		return 0, engine.ErrInvalidInteger
	}
	return nowMillis + deadline, nil
}

func parseZAddArguments(args []string) (engine.ZAddOptions, []engine.ZSetItem, error) {
	options := engine.ZAddOptions{}
	index := 0
	for index < len(args) {
		recognized := true
		switch strings.ToUpper(args[index]) {
		case "NX":
			options.NX = true
		case "XX":
			options.XX = true
		case "GT":
			options.GT = true
		case "LT":
			options.LT = true
		case "CH":
			options.CH = true
		case "INCR":
			options.INCR = true
		default:
			recognized = false
		}
		if !recognized {
			break
		}
		index++
	}
	if index >= len(args) || (len(args)-index)%2 != 0 {
		return engine.ZAddOptions{}, nil, errors.New("syntax error")
	}
	items := make([]engine.ZSetItem, 0, (len(args)-index)/2)
	for ; index < len(args); index += 2 {
		score, err := strconv.ParseFloat(args[index], 64)
		if err != nil || math.IsNaN(score) {
			return engine.ZAddOptions{}, nil, engine.ErrInvalidFloat
		}
		items = append(items, engine.ZSetItem{Score: score, Member: args[index+1]})
	}
	return options, items, nil
}

func executeZRange(db *engine.DB, key, startArg, stopArg string, args []string) ([]engine.ZSetItem, bool, error) {
	query, withScores, err := parseZRangeQuery(startArg, stopArg, args)
	if err != nil {
		return nil, false, err
	}
	items, err := db.ZRangeQuery(key, query)
	return items, withScores, err
}

func parseZRangeQuery(startArg, stopArg string, args []string) (engine.ZRangeQuery, bool, error) {
	mode := "rank"
	reverse := false
	withScores := false
	offset, count := int64(0), int64(-1)
	for index := 0; index < len(args); index++ {
		switch strings.ToUpper(args[index]) {
		case "BYSCORE":
			if mode != "rank" {
				return engine.ZRangeQuery{}, false, errors.New("syntax error")
			}
			mode = "score"
		case "BYLEX":
			if mode != "rank" {
				return engine.ZRangeQuery{}, false, errors.New("syntax error")
			}
			mode = "lex"
		case "REV":
			reverse = true
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if index+2 >= len(args) {
				return engine.ZRangeQuery{}, false, errors.New("syntax error")
			}
			var err error
			offset, err = strconv.ParseInt(args[index+1], 10, 64)
			if err != nil || offset < 0 {
				return engine.ZRangeQuery{}, false, engine.ErrInvalidInteger
			}
			count, err = strconv.ParseInt(args[index+2], 10, 64)
			if err != nil {
				return engine.ZRangeQuery{}, false, engine.ErrInvalidInteger
			}
			index += 2
		default:
			return engine.ZRangeQuery{}, false, errors.New("syntax error")
		}
	}
	if mode == "rank" {
		if offset != 0 || count != -1 {
			return engine.ZRangeQuery{}, false, errors.New("LIMIT is only supported with BYSCORE or BYLEX")
		}
		start, err := strconv.ParseInt(startArg, 10, 64)
		if err != nil {
			return engine.ZRangeQuery{}, false, engine.ErrInvalidInteger
		}
		stop, err := strconv.ParseInt(stopArg, 10, 64)
		if err != nil {
			return engine.ZRangeQuery{}, false, engine.ErrInvalidInteger
		}
		return engine.ZRangeQuery{Mode: engine.ZRangeRank, Start: start, Stop: stop, Reverse: reverse, Count: -1}, withScores, nil
	}
	if reverse {
		startArg, stopArg = stopArg, startArg
	}
	if mode == "score" {
		min, err := parseScoreBound(startArg)
		if err != nil {
			return engine.ZRangeQuery{}, false, err
		}
		max, err := parseScoreBound(stopArg)
		if err != nil {
			return engine.ZRangeQuery{}, false, err
		}
		return engine.ZRangeQuery{Mode: engine.ZRangeScore, MinScore: min, MaxScore: max, Reverse: reverse, Offset: offset, Count: count}, withScores, nil
	}
	min, err := parseLexBound(startArg)
	if err != nil {
		return engine.ZRangeQuery{}, false, err
	}
	max, err := parseLexBound(stopArg)
	if err != nil {
		return engine.ZRangeQuery{}, false, err
	}
	return engine.ZRangeQuery{Mode: engine.ZRangeLex, MinLex: min, MaxLex: max, Reverse: reverse, Offset: offset, Count: count}, withScores, nil
}

func parseScoreBound(value string) (engine.ScoreBound, error) {
	bound := engine.ScoreBound{}
	if strings.HasPrefix(value, "(") {
		bound.Exclusive = true
		value = value[1:]
	}
	score, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(score) {
		return engine.ScoreBound{}, engine.ErrInvalidFloat
	}
	bound.Value = score
	return bound, nil
}

func parseLexBound(value string) (engine.LexBound, error) {
	switch value {
	case "-":
		return engine.LexBound{Infinite: -1}, nil
	case "+":
		return engine.LexBound{Infinite: 1}, nil
	}
	if len(value) < 1 || value[0] != '[' && value[0] != '(' {
		return engine.LexBound{}, errors.New("invalid lex range item")
	}
	return engine.LexBound{Value: value[1:], Exclusive: value[0] == '('}, nil
}

func writeZSetItems(w *proto.Writer, items []engine.ZSetItem, withScores bool) error {
	length := len(items)
	if withScores {
		length *= 2
	}
	if err := w.WriteArrayHeader(length); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.WriteBlobString([]byte(item.Member)); err != nil {
			return err
		}
		if withScores {
			if err := w.WriteFloat(item.Score); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseZCombineArguments(args []string) ([]engine.ZWeightedKey, engine.ZAggregate, bool, error) {
	if len(args) < 2 {
		return nil, 0, false, errors.New("syntax error")
	}
	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys <= 0 || len(args) < numKeys+1 {
		return nil, 0, false, engine.ErrInvalidInteger
	}
	sources := make([]engine.ZWeightedKey, numKeys)
	for index, key := range args[1 : numKeys+1] {
		sources[index] = engine.ZWeightedKey{Key: key, Weight: 1}
	}
	aggregate := engine.ZAggregateSum
	withScores := false
	for index := numKeys + 1; index < len(args); {
		switch strings.ToUpper(args[index]) {
		case "WEIGHTS":
			if index+numKeys >= len(args) {
				return nil, 0, false, errors.New("syntax error")
			}
			for sourceIndex := range sources {
				weight, err := strconv.ParseFloat(args[index+1+sourceIndex], 64)
				if err != nil || math.IsNaN(weight) {
					return nil, 0, false, engine.ErrInvalidFloat
				}
				sources[sourceIndex].Weight = weight
			}
			index += numKeys + 1
		case "AGGREGATE":
			if index+1 >= len(args) {
				return nil, 0, false, errors.New("syntax error")
			}
			switch strings.ToUpper(args[index+1]) {
			case "SUM":
				aggregate = engine.ZAggregateSum
			case "MIN":
				aggregate = engine.ZAggregateMin
			case "MAX":
				aggregate = engine.ZAggregateMax
			default:
				return nil, 0, false, errors.New("syntax error")
			}
			index += 2
		case "WITHSCORES":
			withScores = true
			index++
		default:
			return nil, 0, false, errors.New("syntax error")
		}
	}
	return sources, aggregate, withScores, nil
}

func parseZDiffArguments(args []string) ([]string, bool, error) {
	if len(args) < 2 {
		return nil, false, errors.New("syntax error")
	}
	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys <= 0 || len(args) < numKeys+1 {
		return nil, false, engine.ErrInvalidInteger
	}
	withScores := false
	rest := args[numKeys+1:]
	if len(rest) > 0 {
		if len(rest) != 1 || !strings.EqualFold(rest[0], "WITHSCORES") {
			return nil, false, errors.New("syntax error")
		}
		withScores = true
	}
	return args[1 : numKeys+1], withScores, nil
}

func writeDispatchError(w *proto.Writer, err error) error {
	var coded interface{ Code() string }
	if errors.As(err, &coded) {
		return w.WriteErrorCode(coded.Code(), err.Error())
	}
	if errors.Is(err, errReadOnly) {
		return w.WriteErrorCode("READONLY", err.Error())
	}
	if errors.Is(err, engine.ErrWrongType) {
		return w.WriteErrorCode("WRONGTYPE", "Operation against a key holding the wrong kind of value")
	}
	if errors.Is(err, engine.ErrGroupExists) {
		return w.WriteErrorCode("BUSYGROUP", err.Error())
	}
	if errors.Is(err, engine.ErrNoGroup) {
		return w.WriteErrorCode("NOGROUP", err.Error())
	}
	return w.WriteError(err.Error())
}

func parseRange(startStr, stopStr string) (int, int, error) {
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start %q", startStr)
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid stop %q", stopStr)
	}
	return start, stop, nil
}
