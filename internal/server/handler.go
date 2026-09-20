package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
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
	cmd := strings.ToUpper(tokens[0])
	args := tokens[1:]
	if h.server != nil && h.server.isReplica() {
		if meta, ok := commands[cmd]; ok && !meta.readOnly {
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteBlobString([]byte(args[0]))
	case "SELECT":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		if args[0] != "0" {
			return false, errors.New("only database 0 is supported")
		}
		return false, w.WriteSimpleString("OK")
	case "CLIENT":
		if len(args) >= 1 && strings.EqualFold(args[0], "SETINFO") && len(args) == 3 {
			return false, w.WriteSimpleString("OK")
		}
		return false, errors.New("unsupported CLIENT subcommand")
	case "PING":
		if err := exact(cmd, args, 0); err != nil {
			return false, err
		}
		return false, w.WriteSimpleString("PONG")

	case "QUIT":
		if err := exact(cmd, args, 0); err != nil {
			return false, err
		}
		_ = w.WriteSimpleString("BYE")
		return true, nil

	// ── Generic key commands ─────────────────────────────────────────────────

	case "TYPE":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteSimpleString(h.db.Type(args[0]).String())

	case "DEL":
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteInt(h.db.Del(args...))

	case "EXISTS":
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteInt(h.db.Exists(args...))

	case "RENAME", "RENAMENX":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		renamed, err := h.db.Rename(args[0], args[1], cmd == "RENAMENX")
		if err != nil {
			return false, err
		}
		if cmd == "RENAMENX" {
			return false, w.WriteInt(boolInt(renamed))
		}
		return false, w.WriteSimpleString("OK")

	case "COPY":
		if len(args) < 2 {
			return false, fmt.Errorf("%s requires at least 2 arg(s), got %d", cmd, len(args))
		}
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
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteInt(h.db.Touch(args...))

	case "DBSIZE":
		if err := exact(cmd, args, 0); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		return false, writeBlobArray(w, h.db.Keys(args[0]))

	case "SCAN":
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
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
		if len(args) < 2 || len(args) > 3 {
			return false, fmt.Errorf("%s requires 2 or 3 args, got %d", cmd, len(args))
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteInt64(h.db.TTL(args[0], cmd == "PTTL"))

	case "EXPIRETIME", "PEXPIRETIME":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		return false, w.WriteInt64(h.db.ExpireTime(args[0], cmd == "PEXPIRETIME"))

	case "PERSIST":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		if h.db.Persist(args[0]) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	// ── String ────────────────────────────────────────────────────────────────

	case "SET":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		if h.db.SetNX(args[0], []byte(args[1])) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "GETSET":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		value, found, err := h.db.GetSet(args[0], []byte(args[1]))
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "GETDEL":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		value, found, err := h.db.GetDel(args[0])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "GET":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		value, found, err := h.db.Get(args[0])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString(value)

	case "MSET":
		if len(args) < 2 || len(args)%2 != 0 {
			return false, fmt.Errorf("%s requires one or more key-value pairs", cmd)
		}
		pairs := make([]engine.StringPair, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			pairs = append(pairs, engine.StringPair{Key: args[i], Value: []byte(args[i+1])})
		}
		h.db.MSet(pairs...)
		return false, w.WriteSimpleString("OK")

	case "MSETNX":
		if len(args) < 2 || len(args)%2 != 0 {
			return false, fmt.Errorf("%s requires one or more key-value pairs", cmd)
		}
		pairs := make([]engine.StringPair, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			pairs = append(pairs, engine.StringPair{Key: args[i], Value: []byte(args[i+1])})
		}
		if h.db.MSetNX(pairs...) {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "MGET":
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		n, err := h.db.Append(args[0], []byte(args[1]))
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "STRLEN":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		n, err := h.db.StrLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "GETRANGE":
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := oneOrTwo(cmd, args); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		length, err := h.db.LLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "LRANGE":
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
		index, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return false, engine.ErrInvalidInteger
		}
		if err := h.db.LSet(args[0], index, args[2]); err != nil {
			return false, err
		}
		return false, w.WriteSimpleString("OK")

	case "LTRIM":
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 4); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 4); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		value, found, err := h.db.RPopLPush(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "BLPOP", "BRPOP":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
			return false, w.WriteNil()
		}
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		if err := w.WriteArrayHeader(2); err != nil {
			return false, err
		}
		if err := w.WriteBlobString([]byte(key)); err != nil {
			return false, err
		}
		return false, w.WriteBlobString([]byte(value))

	case "BLMOVE":
		if err := exact(cmd, args, 5); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if len(args) < 3 || len(args)%2 == 0 {
			return false, fmt.Errorf("%s requires one or more field-value pairs", cmd)
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
		stored, err := h.db.HSetNX(args[0], args[1], args[2])
		if err != nil {
			return false, err
		}
		if stored {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "HGET":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		value, found, err := h.db.HGet(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteBlobString([]byte(value))

	case "HMGET":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
		n, err := h.db.HDel(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "HLEN":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		n, err := h.db.HLen(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "HEXISTS":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		found, err := h.db.HExists(args[0], args[1])
		if err != nil {
			return false, err
		}
		if found {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "HKEYS", "HVALS":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		length, err := h.db.HStrLen(args[0], args[1])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(length)

	case "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT":
		if err := atLeast(cmd, args, 4); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		pairs, err := h.db.HGetAll(args[0])
		if err != nil {
			return false, err
		}
		if err := w.WriteArrayHeader(len(pairs) * 2); err != nil {
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
		n, err := h.db.SAdd(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SADDEX":
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
		n, err := h.db.SRem(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SISMEMBER":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		found, err := h.db.SIsMember(args[0], args[1])
		if err != nil {
			return false, err
		}
		if found {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)

	case "SMISMEMBER":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
		moved, err := h.db.SMove(args[0], args[1], args[2])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(moved)

	case "SCARD":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		n, err := h.db.SCard(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "SMEMBERS":
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		members, err := h.db.SMembers(args[0])
		if err != nil {
			return false, err
		}
		return false, writeBlobArray(w, members)

	case "SPOP":
		if err := oneOrTwo(cmd, args); err != nil {
			return false, err
		}
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
		if err := oneOrTwo(cmd, args); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 1); err != nil {
			return false, err
		}
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
		return false, writeBlobArray(w, members)

	case "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
		n, err := h.db.ZRem(args[0], args[1:]...)
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "ZSCORE":
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
		score, found, err := h.db.ZScore(args[0], args[1])
		if err != nil {
			return false, err
		}
		if !found {
			return false, w.WriteNil()
		}
		return false, w.WriteFloat(score)

	case "ZMSCORE":
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 1); err != nil {
			return false, err
		}
		n, err := h.db.ZCard(args[0])
		if err != nil {
			return false, err
		}
		return false, w.WriteInt(n)

	case "ZCOUNT":
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
		items, withScores, err := executeZRange(h.db, args[0], args[1], args[2], args[3:])
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, withScores)

	case "ZRANGESTORE":
		if err := atLeast(cmd, args, 4); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := oneOrTwo(cmd, args); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
			return false, w.WriteNil()
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
		if len(args) < 1 || len(args) > 3 {
			return false, fmt.Errorf("%s requires 1 to 3 args, got %d", cmd, len(args))
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 3); err != nil {
			return false, err
		}
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
		if err := atLeast(cmd, args, 2); err != nil {
			return false, err
		}
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
		if err := exact(cmd, args, 3); err != nil {
			return false, err
		}
		start, stop, err := parseRange(args[1], args[2])
		if err != nil {
			return false, err
		}
		items, err := h.db.ZRange(args[0], start, stop)
		if err != nil {
			return false, err
		}
		return false, writeZSetItems(w, items, true)

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

func exact(cmd string, args []string, n int) error {
	if len(args) != n {
		return fmt.Errorf("%s requires %d arg(s), got %d", cmd, n, len(args))
	}
	return nil
}

func atLeast(cmd string, args []string, n int) error {
	if len(args) < n {
		return fmt.Errorf("%s requires at least %d arg(s), got %d", cmd, n, len(args))
	}
	return nil
}

func oneOrTwo(cmd string, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("%s requires 1 or 2 arg(s), got %d", cmd, len(args))
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
	if errors.Is(err, errReadOnly) {
		return w.WriteErrorCode("READONLY", err.Error())
	}
	if errors.Is(err, engine.ErrWrongType) {
		return w.WriteErrorCode("WRONGTYPE", "Operation against a key holding the wrong kind of value")
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
