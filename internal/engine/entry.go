package engine

import "errors"

var (
	// ErrWrongType is returned when a command addresses an existing key using
	// the wrong data type.
	ErrWrongType = errors.New("wrong type")

	// ErrInvalidInteger is returned when a command argument or stored value
	// cannot be represented as a signed 64-bit integer.
	ErrInvalidInteger = errors.New("value is not an integer or out of range")

	// ErrIndexOutOfRange is returned when a list index does not identify an
	// existing element.
	ErrIndexOutOfRange = errors.New("index out of range")

	// ErrInvalidDirection is returned for an unsupported list end.
	ErrInvalidDirection = errors.New("invalid list direction")

	// ErrNoKeys is returned when a multi-key operation has no source keys.
	ErrNoKeys = errors.New("at least one key is required")

	ErrInvalidFloat     = errors.New("value is not a valid float")
	ErrInvalidOffset    = errors.New("offset is out of range")
	ErrInvalidOptions   = errors.New("incompatible options")
	ErrInvalidStreamID  = errors.New("invalid stream ID")
	ErrStreamIDTooSmall = errors.New("stream ID is equal to or smaller than the target stream top item")
	ErrStreamIDOverflow = errors.New("stream ID sequence overflow")
	ErrGroupExists      = errors.New("consumer group already exists")
	ErrNoGroup          = errors.New("consumer group does not exist")

	// ErrNoSuchKey is returned when an operation requires an existing source.
	ErrNoSuchKey = errors.New("no such key")

	// ErrSameKey is returned when an operation requires distinct source and
	// destination keys.
	ErrSameKey = errors.New("source and destination objects are the same")
)

// Kind identifies the value type stored at a key.
type Kind uint8

const (
	KindNone Kind = iota
	KindString
	KindHash
	KindList
	KindSet
	KindSortedSet
	KindStream
	KindJSON
)

func (k Kind) String() string {
	switch k {
	case KindNone:
		return "none"
	case KindString:
		return "string"
	case KindHash:
		return "hash"
	case KindList:
		return "list"
	case KindSet:
		return "set"
	case KindSortedSet:
		return "zset"
	case KindStream:
		return "stream"
	case KindJSON:
		return "ReJSON-RL"
	default:
		return "unknown"
	}
}

// entry is the canonical value stored for a key. Value's concrete type is
// determined by Kind and is only accessed while DB.mu is held.
type entry struct {
	kind       Kind
	value      any
	expireAt   int64
	lastAccess int64
}
