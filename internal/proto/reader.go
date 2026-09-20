package proto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrEmptyCommand is returned when a blank line is received.
var ErrEmptyCommand = errors.New("empty command")

// ReadCommand reads one newline-terminated line from r and returns its tokens.
// Tokens are space-separated; values containing spaces must be double-quoted.
// Supports backslash escapes: \" \\ \n \r
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			// last line with no trailing newline — still valid
		} else {
			return nil, err
		}
	}
	line = strings.TrimRight(line, "\r\n")
	tokens, err := tokenize(line)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, ErrEmptyCommand
	}
	return tokens, nil
}

// ReadRequest accepts either a KLP line or a RESP command array. RESP arrays
// must contain bulk or simple strings; bulk payloads are read by length so
// embedded NULs and line breaks are preserved.
func ReadRequest(r *bufio.Reader) ([]string, Mode, error) {
	first, err := r.Peek(1)
	if err != nil {
		return nil, ModeKLP, err
	}
	if first[0] != '*' {
		tokens, err := ReadCommand(r)
		return tokens, ModeKLP, err
	}
	line, err := readRESPLine(r)
	if err != nil {
		return nil, ModeRESP2, err
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil || count <= 0 || count > 1<<20 {
		return nil, ModeRESP2, errors.New("invalid RESP command array length")
	}
	tokens := make([]string, count)
	for i := range tokens {
		line, err = readRESPLine(r)
		if err != nil {
			return nil, ModeRESP2, err
		}
		if len(line) == 0 {
			return nil, ModeRESP2, errors.New("empty RESP command element")
		}
		switch line[0] {
		case '+':
			tokens[i] = line[1:]
		case '$':
			size, parseErr := strconv.Atoi(line[1:])
			if parseErr != nil || size < 0 || size > 512<<20 {
				return nil, ModeRESP2, errors.New("invalid RESP bulk length")
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(r, payload); err != nil {
				return nil, ModeRESP2, err
			}
			if payload[size] != '\r' || payload[size+1] != '\n' {
				return nil, ModeRESP2, errors.New("invalid RESP bulk terminator")
			}
			tokens[i] = string(payload[:size])
		default:
			return nil, ModeRESP2, errors.New("RESP command elements must be strings")
		}
	}
	return tokens, ModeRESP2, nil
}

func readRESPLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("RESP line must end in CRLF")
	}
	return line[:len(line)-2], nil
}

func tokenize(line string) ([]string, error) {
	var tokens []string
	i := 0
	for i < len(line) {
		// skip whitespace
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}

		if line[i] == '"' {
			i++ // skip opening quote
			var buf strings.Builder
			closed := false
			for i < len(line) {
				if line[i] == '\\' && i+1 < len(line) {
					i++
					switch line[i] {
					case '"':
						buf.WriteByte('"')
					case '\\':
						buf.WriteByte('\\')
					case 'n':
						buf.WriteByte('\n')
					case 'r':
						buf.WriteByte('\r')
					default:
						buf.WriteByte('\\')
						buf.WriteByte(line[i])
					}
					i++
				} else if line[i] == '"' {
					i++ // skip closing quote
					closed = true
					break
				} else {
					buf.WriteByte(line[i])
					i++
				}
			}
			if !closed {
				return nil, errors.New("unclosed quote")
			}
			tokens = append(tokens, buf.String())
		} else {
			start := i
			for i < len(line) && line[i] != ' ' && line[i] != '\t' {
				i++
			}
			tokens = append(tokens, line[start:i])
		}
	}
	return tokens, nil
}

// ── Response reader (used by the client) ─────────────────────────────────────

// ResponseType tags the kind of value in a Response.
type ResponseType int

const (
	RSimpleString ResponseType = iota
	RBlobString
	RInteger
	RFloat
	RNil
	RError
	RArray
	RMap
)

// Response holds one parsed server response.
type Response struct {
	Type     ResponseType
	Str      string  // RSimpleString, RBlobString, RError
	Int      int64   // RInteger
	Float    float64 // RFloat
	Elements []Response
}

// ReadResponse reads one complete response from r (including nested array elements).
func ReadResponse(r *bufio.Reader) (Response, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return Response{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return Response{}, errors.New("empty response line")
	}

	sigil := line[0]
	rest := line[1:]

	switch sigil {
	case '+':
		return Response{Type: RSimpleString, Str: rest}, nil

	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 {
			return Response{}, fmt.Errorf("invalid blob string length: %s", rest)
		}
		if n == -1 {
			return Response{Type: RNil}, nil
		}
		value := make([]byte, n)
		if _, err := io.ReadFull(r, value); err != nil {
			return Response{}, err
		}
		terminator, err := r.ReadByte()
		if err != nil {
			return Response{}, err
		}
		if terminator == '\r' {
			terminator, err = r.ReadByte()
			if err != nil {
				return Response{}, err
			}
		}
		if terminator != '\n' {
			return Response{}, errors.New("blob string is missing line terminator")
		}
		return Response{Type: RBlobString, Str: string(value)}, nil

	case '-':
		return Response{Type: RError, Str: rest}, nil

	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return Response{}, fmt.Errorf("invalid integer response: %s", rest)
		}
		return Response{Type: RInteger, Int: n}, nil

	case ',':
		f, err := strconv.ParseFloat(rest, 64)
		if err != nil {
			return Response{}, fmt.Errorf("invalid float response: %s", rest)
		}
		return Response{Type: RFloat, Float: f}, nil

	case '_':
		return Response{Type: RNil}, nil

	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 {
			return Response{}, fmt.Errorf("invalid array length: %s", rest)
		}
		if n == -1 {
			return Response{Type: RNil}, nil
		}
		elems := make([]Response, n)
		for i := 0; i < n; i++ {
			elems[i], err = ReadResponse(r)
			if err != nil {
				return Response{}, err
			}
		}
		return Response{Type: RArray, Elements: elems}, nil

	case '%':
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 {
			return Response{}, fmt.Errorf("invalid map length: %s", rest)
		}
		elems := make([]Response, 2*n)
		for i := range elems {
			elems[i], err = ReadResponse(r)
			if err != nil {
				return Response{}, err
			}
		}
		return Response{Type: RMap, Elements: elems}, nil

	default:
		return Response{}, fmt.Errorf("unknown response sigil: %q", sigil)
	}
}
