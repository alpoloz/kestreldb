package server

import (
	"errors"
	"strconv"
	"strings"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func parseBitmapOffset(raw string) (int64, error) {
	offset, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || offset < 0 {
		return 0, engine.ErrBitmapRange
	}
	return offset, nil
}

func (h *handler) writeBitmapCommand(w *proto.Writer, cmd string, args []string) error {
	key := args[0]
	switch cmd {
	case "GETBIT", "SETBIT":
		offset, err := parseBitmapOffset(args[1])
		if err != nil {
			return err
		}
		if cmd == "GETBIT" {
			value, err := h.db.GetBit(key, offset)
			if err != nil {
				return err
			}
			return w.WriteInt(value)
		}
		bit, err := strconv.Atoi(args[2])
		if err != nil || bit != 0 && bit != 1 {
			return errors.New("bit is not an integer or out of range")
		}
		previous, err := h.db.SetBit(key, offset, bit)
		if err != nil {
			return err
		}
		return w.WriteInt(previous)
	case "BITCOUNT":
		if len(args) == 2 {
			return errors.New("syntax error")
		}
		var start, end int64
		var err error
		bitUnit := false
		if len(args) >= 3 {
			start, err = strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return engine.ErrInvalidInteger
			}
			end, err = strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return engine.ErrInvalidInteger
			}
		}
		if len(args) == 4 {
			if !strings.EqualFold(args[3], "BIT") && !strings.EqualFold(args[3], "BYTE") {
				return errors.New("syntax error")
			}
			bitUnit = strings.EqualFold(args[3], "BIT")
		}
		count, err := h.db.BitCount(key, start, end, len(args) >= 3, bitUnit)
		if err != nil {
			return err
		}
		return w.WriteInt(count)
	case "BITPOS":
		bit, err := strconv.Atoi(args[1])
		if err != nil || bit != 0 && bit != 1 {
			return errors.New("bit is not an integer or out of range")
		}
		if len(args) == 5 && !strings.EqualFold(args[4], "BIT") && !strings.EqualFold(args[4], "BYTE") {
			return errors.New("syntax error")
		}
		var start, end int64
		if len(args) >= 3 {
			start, err = strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return engine.ErrInvalidInteger
			}
		}
		if len(args) >= 4 {
			end, err = strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return engine.ErrInvalidInteger
			}
		}
		position, err := h.db.BitPos(key, bit, start, end, len(args) >= 3, len(args) >= 4, len(args) == 5 && strings.EqualFold(args[4], "BIT"))
		if err != nil {
			return err
		}
		return w.WriteInt64(position)
	case "BITOP":
		length, err := h.db.BitOp(strings.ToUpper(args[0]), args[1], args[2:]...)
		if err != nil {
			return err
		}
		return w.WriteInt(length)
	case "BITFIELD", "BITFIELD_RO":
		ops, err := parseBitFieldOps(args[1:], cmd == "BITFIELD_RO")
		if err != nil {
			return err
		}
		results, err := h.db.BitField(key, ops, cmd == "BITFIELD_RO")
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(len(results)); err != nil {
			return err
		}
		for _, result := range results {
			if result.Nil {
				err = w.WriteNil()
			} else {
				err = w.WriteInt64(result.Value)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("unsupported bitmap command")
}

func parseBitFieldOps(args []string, readonly bool) ([]engine.BitFieldOp, error) {
	var ops []engine.BitFieldOp
	overflow := "WRAP"
	for i := 0; i < len(args); {
		kind := strings.ToUpper(args[i])
		i++
		if kind == "OVERFLOW" {
			if readonly || i >= len(args) {
				return nil, errors.New("syntax error")
			}
			overflow = strings.ToUpper(args[i])
			i++
			if overflow != "WRAP" && overflow != "SAT" && overflow != "FAIL" {
				return nil, errors.New("syntax error")
			}
			continue
		}
		if kind != "GET" && kind != "SET" && kind != "INCRBY" || readonly && kind != "GET" {
			return nil, errors.New("syntax error")
		}
		needed := 2
		if kind != "GET" {
			needed = 3
		}
		if i+needed > len(args) {
			return nil, errors.New("syntax error")
		}
		format := args[i]
		i++
		if len(format) < 2 || format[0] != 'i' && format[0] != 'u' {
			return nil, errors.New("syntax error")
		}
		width, err := strconv.Atoi(format[1:])
		if err != nil || width < 1 || width > 64 || format[0] == 'u' && width == 64 {
			return nil, errors.New("invalid bitfield type")
		}
		rawOffset := args[i]
		i++
		indexed := strings.HasPrefix(rawOffset, "#")
		if indexed {
			rawOffset = rawOffset[1:]
		}
		offset, err := parseBitmapOffset(rawOffset)
		if err != nil {
			return nil, err
		}
		if indexed {
			if offset > (1<<63-1)/int64(width) {
				return nil, engine.ErrBitmapRange
			}
			offset *= int64(width)
		}
		op := engine.BitFieldOp{Kind: kind, Signed: format[0] == 'i', Width: width, Offset: offset, Overflow: overflow}
		if kind != "GET" {
			op.Value, err = strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return nil, engine.ErrInvalidInteger
			}
			i++
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return nil, errors.New("syntax error")
	}
	return ops, nil
}
