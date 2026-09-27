package server

import (
	"errors"
	"strings"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func jsonPathArg(args []string) string {
	if len(args) > 1 {
		return args[1]
	}
	return "."
}

func writeJSONScalar(w *proto.Writer, path string, data []byte, found bool) error {
	if !found {
		if strings.HasPrefix(path, "$") {
			return w.WriteBlobString([]byte("[]"))
		}
		return w.WriteNil()
	}
	if strings.HasPrefix(path, "$") {
		wrapped := make([]byte, 0, len(data)+2)
		wrapped = append(wrapped, '[')
		wrapped = append(wrapped, data...)
		wrapped = append(wrapped, ']')
		return w.WriteBlobString(wrapped)
	}
	return w.WriteBlobString(data)
}

func writeJSONInt(w *proto.Writer, path string, value int, found bool) error {
	if strings.HasPrefix(path, "$") {
		if !found {
			return w.WriteArrayHeader(0)
		}
		if err := w.WriteArrayHeader(1); err != nil {
			return err
		}
		return w.WriteInt(value)
	}
	if !found {
		return w.WriteNil()
	}
	return w.WriteInt(value)
}

func (h *handler) writeJSONCommand(w *proto.Writer, cmd string, args []string) error {
	key := args[0]
	switch cmd {
	case "JSON.SET":
		nx, xx := false, false
		if len(args) == 4 {
			switch strings.ToUpper(args[3]) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			default:
				return errors.New("syntax error")
			}
		}
		stored, err := h.db.JSONSet(key, args[1], []byte(args[2]), nx, xx)
		if err != nil {
			return err
		}
		if !stored {
			return w.WriteNil()
		}
		return w.WriteSimpleString("OK")
	case "JSON.GET":
		path := jsonPathArg(args)
		data, found, err := h.db.JSONGet(key, path)
		if err != nil {
			return err
		}
		return writeJSONScalar(w, path, data, found)
	case "JSON.DEL", "JSON.FORGET":
		count, err := h.db.JSONDel(key, jsonPathArg(args))
		if err != nil {
			return err
		}
		return w.WriteInt(count)
	case "JSON.TYPE":
		path := jsonPathArg(args)
		kind, found, err := h.db.JSONType(key, path)
		if err != nil {
			return err
		}
		if strings.HasPrefix(path, "$") {
			if !found {
				return w.WriteArrayHeader(0)
			}
			if err := w.WriteArrayHeader(1); err != nil {
				return err
			}
		} else if !found {
			return w.WriteNil()
		}
		return w.WriteBlobString([]byte(kind))
	case "JSON.CLEAR":
		count, err := h.db.JSONClear(key, jsonPathArg(args))
		if err != nil {
			return err
		}
		return w.WriteInt(count)
	case "JSON.NUMINCRBY":
		data, found, err := h.db.JSONNumIncrBy(key, args[1], args[2])
		if err != nil {
			return err
		}
		return writeJSONScalar(w, args[1], data, found)
	case "JSON.STRAPPEND":
		length, found, err := h.db.JSONStrAppend(key, args[1], []byte(args[2]))
		if err != nil {
			return err
		}
		return writeJSONInt(w, args[1], length, found)
	case "JSON.ARRAPPEND":
		raw := make([][]byte, len(args)-2)
		for i := range raw {
			raw[i] = []byte(args[i+2])
		}
		length, found, err := h.db.JSONArrAppend(key, args[1], raw...)
		if err != nil {
			return err
		}
		return writeJSONInt(w, args[1], length, found)
	case "JSON.ARRLEN", "JSON.OBJLEN":
		path := jsonPathArg(args)
		var length int
		var found bool
		var err error
		if cmd == "JSON.ARRLEN" {
			length, found, err = h.db.JSONArrLen(key, path)
		} else {
			length, found, err = h.db.JSONObjLen(key, path)
		}
		if err != nil {
			if errors.Is(err, engine.ErrWrongType) && strings.HasPrefix(path, "$") {
				return w.WriteArrayHeader(0)
			}
			return err
		}
		return writeJSONInt(w, path, length, found)
	case "JSON.OBJKEYS":
		path := jsonPathArg(args)
		keys, found, err := h.db.JSONObjKeys(key, path)
		if err != nil {
			return err
		}
		if !found {
			if strings.HasPrefix(path, "$") {
				return w.WriteArrayHeader(0)
			}
			return w.WriteNil()
		}
		if strings.HasPrefix(path, "$") {
			if err := w.WriteArrayHeader(1); err != nil {
				return err
			}
		}
		return writeBlobArray(w, keys)
	}
	return errors.New("unsupported JSON command")
}
