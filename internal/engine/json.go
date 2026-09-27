package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrInvalidJSON = errors.New("invalid JSON value")
var ErrInvalidJSONPath = errors.New("invalid JSON path")

type jsonStep struct {
	field string
	index int
	array bool
}

func parseJSONValue(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, ErrInvalidJSON
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, ErrInvalidJSON
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, ErrInvalidJSON
	}
	return value, nil
}

func cloneJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		copyValue := make(map[string]any, len(v))
		for key, child := range v {
			copyValue[key] = cloneJSONValue(child)
		}
		return copyValue
	case []any:
		copyValue := make([]any, len(v))
		for i, child := range v {
			copyValue[i] = cloneJSONValue(child)
		}
		return copyValue
	default:
		return value
	}
}

// JSON paths are definite paths rooted at $ or .; wildcard and recursive paths
// are intentionally left for a later JSONPath milestone.
func parseJSONPath(path string) ([]jsonStep, error) {
	if path == "$" || path == "." {
		return nil, nil
	}
	if len(path) == 0 || path[0] != '$' && path[0] != '.' {
		return nil, ErrInvalidJSONPath
	}
	i := 1
	if path[0] == '.' && i < len(path) && path[i] != '.' && path[i] != '[' {
		i = 0
	}
	var steps []jsonStep
	for i < len(path) {
		switch path[i] {
		case '.':
			i++
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			if start == i {
				return nil, ErrInvalidJSONPath
			}
			steps = append(steps, jsonStep{field: path[start:i]})
		case '[':
			i++
			if i >= len(path) {
				return nil, ErrInvalidJSONPath
			}
			if path[i] == '\'' || path[i] == '"' {
				quote := path[i]
				i++
				start := i
				for i < len(path) && path[i] != quote {
					i++
				}
				if i >= len(path) || i+1 >= len(path) || path[i+1] != ']' {
					return nil, ErrInvalidJSONPath
				}
				steps = append(steps, jsonStep{field: path[start:i]})
				i += 2
			} else {
				start := i
				for i < len(path) && path[i] != ']' {
					i++
				}
				if i >= len(path) {
					return nil, ErrInvalidJSONPath
				}
				index, err := strconv.Atoi(path[start:i])
				if err != nil {
					return nil, ErrInvalidJSONPath
				}
				steps = append(steps, jsonStep{index: index, array: true})
				i++
			}
		default:
			return nil, ErrInvalidJSONPath
		}
	}
	return steps, nil
}

func jsonIndex(index, length int) int {
	if index < 0 {
		return length + index
	}
	return index
}

func jsonAt(value any, steps []jsonStep) (any, bool) {
	for _, step := range steps {
		if step.array {
			array, ok := value.([]any)
			if !ok {
				return nil, false
			}
			i := jsonIndex(step.index, len(array))
			if i < 0 || i >= len(array) {
				return nil, false
			}
			value = array[i]
		} else {
			object, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			var exists bool
			value, exists = object[step.field]
			if !exists {
				return nil, false
			}
		}
	}
	return value, true
}

func jsonReplace(value any, steps []jsonStep, replacement any) (any, bool) {
	if len(steps) == 0 {
		return replacement, true
	}
	step := steps[0]
	if step.array {
		array, ok := value.([]any)
		if !ok {
			return value, false
		}
		i := jsonIndex(step.index, len(array))
		if i < 0 || i >= len(array) {
			return value, false
		}
		child, changed := jsonReplace(array[i], steps[1:], replacement)
		if changed {
			array[i] = child
		}
		return array, changed
	}
	object, ok := value.(map[string]any)
	if !ok {
		return value, false
	}
	child, exists := object[step.field]
	if !exists && len(steps) > 1 {
		return value, false
	}
	updated, changed := jsonReplace(child, steps[1:], replacement)
	if changed {
		object[step.field] = updated
	}
	return object, changed
}

func jsonRemove(value any, steps []jsonStep) (any, bool) {
	if len(steps) == 0 {
		return nil, true
	}
	step := steps[0]
	if step.array {
		array, ok := value.([]any)
		if !ok {
			return value, false
		}
		i := jsonIndex(step.index, len(array))
		if i < 0 || i >= len(array) {
			return value, false
		}
		if len(steps) == 1 {
			return append(array[:i], array[i+1:]...), true
		}
		child, changed := jsonRemove(array[i], steps[1:])
		if changed {
			array[i] = child
		}
		return array, changed
	}
	object, ok := value.(map[string]any)
	if !ok {
		return value, false
	}
	child, exists := object[step.field]
	if !exists {
		return value, false
	}
	if len(steps) == 1 {
		delete(object, step.field)
		return object, true
	}
	updated, changed := jsonRemove(child, steps[1:])
	if changed {
		object[step.field] = updated
	}
	return object, changed
}

func (db *DB) JSONSet(key, path string, raw []byte, nx, xx bool) (bool, error) {
	steps, err := parseJSONPath(path)
	if err != nil {
		return false, err
	}
	value, err := parseJSONValue(raw)
	if err != nil {
		return false, err
	}
	if nx && xx {
		return false, ErrInvalidOptions
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).JSONSet(key, path, raw, nx, xx)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if exists && e.kind != KindJSON {
		return false, ErrWrongType
	}
	if !exists {
		if xx || len(steps) != 0 {
			return false, nil
		}
		db.entries[key] = &entry{kind: KindJSON, value: value}
		return true, nil
	}
	_, pathExists := jsonAt(e.value, steps)
	if nx && pathExists || xx && !pathExists {
		return false, nil
	}
	updated, changed := jsonReplace(cloneJSONValue(e.value), steps, value)
	if !changed {
		return false, nil
	}
	e.value = updated
	return true, nil
}

func (db *DB) JSONGet(key, path string) ([]byte, bool, error) {
	steps, err := parseJSONPath(path)
	if err != nil {
		return nil, false, err
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).JSONGet(key, path)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if !exists {
		return nil, false, nil
	}
	if e.kind != KindJSON {
		return nil, false, ErrWrongType
	}
	value, found := jsonAt(e.value, steps)
	if !found {
		return nil, false, nil
	}
	data, err := json.Marshal(value)
	return data, true, err
}

func (db *DB) JSONDel(key, path string) (int, error) {
	steps, err := parseJSONPath(path)
	if err != nil {
		return 0, err
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).JSONDel(key, path)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if !exists {
		return 0, nil
	}
	if e.kind != KindJSON {
		return 0, ErrWrongType
	}
	if len(steps) == 0 {
		db.removeEntryLocked(key)
		return 1, nil
	}
	updated, removed := jsonRemove(cloneJSONValue(e.value), steps)
	if removed {
		e.value = updated
		return 1, nil
	}
	return 0, nil
}

func jsonType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number:
		if strings.ContainsAny(string(value.(json.Number)), ".eE") {
			return "number"
		}
		return "integer"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

func (db *DB) JSONType(key, path string) (string, bool, error) {
	data, found, err := db.JSONGet(key, path)
	if err != nil || !found {
		return "", found, err
	}
	value, err := parseJSONValue(data)
	if err != nil {
		return "", false, err
	}
	return jsonType(value), true, nil
}

func (db *DB) JSONClear(key, path string) (int, error) {
	return db.jsonMutate(key, path, func(value any) (any, bool, error) {
		switch v := value.(type) {
		case map[string]any:
			if len(v) == 0 {
				return v, false, nil
			}
			return map[string]any{}, true, nil
		case []any:
			if len(v) == 0 {
				return v, false, nil
			}
			return []any{}, true, nil
		case json.Number:
			if v == "0" {
				return v, false, nil
			}
			return json.Number("0"), true, nil
		default:
			return value, false, nil
		}
	})
}

// jsonMutate validates a path and computes a replacement before changing the
// stored tree, so a failed command cannot leave partial state behind.
func (db *DB) jsonMutate(key, path string, mutate func(any) (any, bool, error)) (int, error) {
	steps, err := parseJSONPath(path)
	if err != nil {
		return 0, err
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).jsonMutate(key, path, mutate)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if !exists {
		return 0, nil
	}
	if e.kind != KindJSON {
		return 0, ErrWrongType
	}
	old, found := jsonAt(e.value, steps)
	if !found {
		return 0, nil
	}
	updated, changed, err := mutate(old)
	if err != nil {
		return 0, err
	}
	if !changed {
		return 0, nil
	}
	newRoot, _ := jsonReplace(cloneJSONValue(e.value), steps, updated)
	e.value = newRoot
	return 1, nil
}

func (db *DB) JSONNumIncrBy(key, path, increment string) ([]byte, bool, error) {
	inc, err := strconv.ParseFloat(increment, 64)
	if err != nil || math.IsInf(inc, 0) || math.IsNaN(inc) {
		return nil, false, ErrInvalidFloat
	}
	var result []byte
	count, err := db.jsonMutate(key, path, func(value any) (any, bool, error) {
		number, ok := value.(json.Number)
		if !ok {
			return value, false, ErrWrongType
		}
		current, err := number.Float64()
		if err != nil {
			return value, false, ErrInvalidFloat
		}
		next := current + inc
		if math.IsInf(next, 0) || math.IsNaN(next) {
			return value, false, ErrInvalidFloat
		}
		formatted := strconv.FormatFloat(next, 'f', -1, 64)
		result = []byte(formatted)
		return json.Number(formatted), true, nil
	})
	return result, count == 1, err
}

func (db *DB) JSONStrAppend(key, path string, raw []byte) (int, bool, error) {
	value, err := parseJSONValue(raw)
	if err != nil {
		return 0, false, err
	}
	appendValue, ok := value.(string)
	if !ok {
		return 0, false, ErrWrongType
	}
	length := 0
	count, err := db.jsonMutate(key, path, func(value any) (any, bool, error) {
		old, ok := value.(string)
		if !ok {
			return value, false, ErrWrongType
		}
		result := old + appendValue
		length = utf8.RuneCountInString(result)
		return result, true, nil
	})
	return length, count == 1, err
}

func (db *DB) JSONArrAppend(key, path string, rawValues ...[]byte) (int, bool, error) {
	values := make([]any, len(rawValues))
	for i, raw := range rawValues {
		value, err := parseJSONValue(raw)
		if err != nil {
			return 0, false, err
		}
		values[i] = value
	}
	length := 0
	count, err := db.jsonMutate(key, path, func(value any) (any, bool, error) {
		array, ok := value.([]any)
		if !ok {
			return value, false, ErrWrongType
		}
		updated := append(append([]any{}, array...), values...)
		length = len(updated)
		return updated, len(values) > 0, nil
	})
	return length, count == 1, err
}

func (db *DB) jsonMeasure(key, path string, kind string) (int, bool, error) {
	data, found, err := db.JSONGet(key, path)
	if err != nil || !found {
		return 0, found, err
	}
	value, err := parseJSONValue(data)
	if err != nil {
		return 0, false, err
	}
	switch kind {
	case "array":
		if array, ok := value.([]any); ok {
			return len(array), true, nil
		}
	case "object":
		if object, ok := value.(map[string]any); ok {
			return len(object), true, nil
		}
	}
	return 0, false, ErrWrongType
}

func (db *DB) JSONArrLen(key, path string) (int, bool, error) {
	return db.jsonMeasure(key, path, "array")
}
func (db *DB) JSONObjLen(key, path string) (int, bool, error) {
	return db.jsonMeasure(key, path, "object")
}

func (db *DB) JSONObjKeys(key, path string) ([]string, bool, error) {
	data, found, err := db.JSONGet(key, path)
	if err != nil || !found {
		return nil, found, err
	}
	value, err := parseJSONValue(data)
	if err != nil {
		return nil, false, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, false, ErrWrongType
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, true, nil
}
