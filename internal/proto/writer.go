package proto

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// Writer sends typed KLP responses over a buffered writer.
// Caller must call Flush() after each command response.
type Writer struct {
	w    *bufio.Writer
	mode Mode
}

type Mode uint8

const (
	ModeKLP Mode = iota
	ModeRESP2
	ModeRESP3
)

func NewWriter(w *bufio.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) SetMode(mode Mode) { w.mode = mode }

func (w *Writer) Mode() Mode { return w.mode }

func (w *Writer) terminator() string {
	if w.mode == ModeKLP {
		return "\n"
	}
	return "\r\n"
}

func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s%s", s, w.terminator())
	return err
}

// WriteBlobString sends a length-prefixed, binary-safe string value.
func (w *Writer) WriteBlobString(value []byte) error {
	if _, err := fmt.Fprintf(w.w, "$%d%s", len(value), w.terminator()); err != nil {
		return err
	}
	if _, err := w.w.Write(value); err != nil {
		return err
	}
	_, err := w.w.WriteString(w.terminator())
	return err
}

func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-ERR %s%s", msg, w.terminator())
	return err
}

// WriteErrorCode sends an error with a protocol-level code such as WRONGTYPE.
func (w *Writer) WriteErrorCode(code, msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s %s%s", code, msg, w.terminator())
	return err
}

func (w *Writer) WriteInt(n int) error {
	_, err := fmt.Fprintf(w.w, ":%d%s", n, w.terminator())
	return err
}

func (w *Writer) WriteInt64(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d%s", n, w.terminator())
	return err
}

func (w *Writer) WriteFloat(f float64) error {
	value := strconv.FormatFloat(f, 'g', -1, 64)
	if w.mode == ModeRESP2 {
		return w.WriteBlobString([]byte(value))
	}
	_, err := fmt.Fprintf(w.w, ",%s%s", value, w.terminator())
	return err
}

func (w *Writer) WriteNil() error {
	if w.mode == ModeRESP2 {
		_, err := w.w.WriteString("$-1\r\n")
		return err
	}
	_, err := w.w.WriteString("_" + w.terminator())
	return err
}

func (w *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.w, "*%d%s", n, w.terminator())
	return err
}

func (w *Writer) WriteMapHeader(n int) error {
	if w.mode != ModeRESP3 {
		return w.WriteArrayHeader(2 * n)
	}
	_, err := fmt.Fprintf(w.w, "%%%d\r\n", n)
	return err
}

func (w *Writer) Flush() error {
	return w.w.Flush()
}

// ── Command formatter (used by the client) ────────────────────────────────────

// FormatCommand encodes tokens as a single KLP command line.
// Tokens containing spaces, tabs, quotes, or backslashes are double-quoted.
func FormatCommand(tokens []string) string {
	var sb strings.Builder
	for i, t := range tokens {
		if i > 0 {
			sb.WriteByte(' ')
		}
		if needsQuoting(t) {
			sb.WriteByte('"')
			for j := 0; j < len(t); j++ {
				switch t[j] {
				case '"':
					sb.WriteString(`\"`)
				case '\\':
					sb.WriteString(`\\`)
				case '\n':
					sb.WriteString(`\n`)
				case '\r':
					sb.WriteString(`\r`)
				default:
					sb.WriteByte(t[j])
				}
			}
			sb.WriteByte('"')
		} else {
			sb.WriteString(t)
		}
	}
	sb.WriteByte('\n')
	return sb.String()
}

// FormatRESPCommand encodes a binary-safe RESP command array.
func FormatRESPCommand(tokens []string) []byte {
	var out strings.Builder
	fmt.Fprintf(&out, "*%d\r\n", len(tokens))
	for _, token := range tokens {
		fmt.Fprintf(&out, "$%d\r\n", len(token))
		out.WriteString(token)
		out.WriteString("\r\n")
	}
	return []byte(out.String())
}

func needsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}
