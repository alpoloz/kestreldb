package engine

// ListDirection identifies one end of a list.
type ListDirection uint8

const (
	ListLeft ListDirection = iota
	ListRight
)

// deque is a dynamically growing circular buffer. Elements are addressed in
// logical left-to-right order independently of the physical head offset.
type deque struct {
	items  []string
	head   int
	length int
}

func newDeque(values ...string) *deque {
	d := &deque{}
	d.replace(values)
	return d
}

func (d *deque) len() int {
	return d.length
}

func (d *deque) at(index int) string {
	return d.items[(d.head+index)%len(d.items)]
}

func (d *deque) set(index int, value string) {
	d.items[(d.head+index)%len(d.items)] = value
}

func (d *deque) pushLeft(value string) {
	d.growIfFull()
	d.head = (d.head - 1 + len(d.items)) % len(d.items)
	d.items[d.head] = value
	d.length++
}

func (d *deque) pushRight(value string) {
	d.growIfFull()
	index := (d.head + d.length) % len(d.items)
	d.items[index] = value
	d.length++
}

func (d *deque) popLeft() string {
	value := d.items[d.head]
	d.items[d.head] = ""
	d.head = (d.head + 1) % len(d.items)
	d.length--
	if d.length == 0 {
		d.head = 0
	}
	return value
}

func (d *deque) popRight() string {
	index := (d.head + d.length - 1) % len(d.items)
	value := d.items[index]
	d.items[index] = ""
	d.length--
	if d.length == 0 {
		d.head = 0
	}
	return value
}

func (d *deque) values() []string {
	values := make([]string, d.length)
	for i := range values {
		values[i] = d.at(i)
	}
	return values
}

func (d *deque) replace(values []string) {
	capacity := 4
	for capacity < len(values) {
		capacity *= 2
	}
	d.items = make([]string, capacity)
	copy(d.items, values)
	d.head = 0
	d.length = len(values)
}

func (d *deque) insert(index int, value string) {
	values := d.values()
	values = append(values, "")
	copy(values[index+1:], values[index:len(values)-1])
	values[index] = value
	d.replace(values)
}

func (d *deque) growIfFull() {
	if d.length < len(d.items) {
		return
	}
	capacity := len(d.items) * 2
	if capacity == 0 {
		capacity = 4
	}
	items := make([]string, capacity)
	for i := 0; i < d.length; i++ {
		items[i] = d.at(i)
	}
	d.items = items
	d.head = 0
}

// LPush prepends values to the list at key and returns its new length. Values
// are pushed from left to right, so the last argument becomes the first item.
func (db *DB) LPush(key string, values ...string) (int, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		n, err := db.shardFor(key).LPush(key, values...)
		release()
		if err == nil {
			db.serveShardedWaitersLocked(key)
		}
		return n, err
	}
	return db.pushList(key, ListLeft, values)
}

// RPush appends values to the list at key and returns its new length.
func (db *DB) RPush(key string, values ...string) (int, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		n, err := db.shardFor(key).RPush(key, values...)
		release()
		if err == nil {
			db.serveShardedWaitersLocked(key)
		}
		return n, err
	}
	return db.pushList(key, ListRight, values)
}

func (db *DB) pushList(key string, direction ListDirection, values []string) (int, error) {
	if !validListDirection(direction) {
		return 0, ErrInvalidDirection
	}
	if len(values) == 0 {
		return 0, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		e = &entry{kind: KindList, value: newDeque()}
		db.entries[key] = e
	}
	if e.kind != KindList {
		return 0, ErrWrongType
	}

	list := e.value.(*deque)
	for _, value := range values {
		if direction == ListLeft {
			list.pushLeft(value)
		} else {
			list.pushRight(value)
		}
	}
	length := list.len()
	db.serveListWaitersLocked(key)
	return length, nil
}

// LPop removes and returns up to count elements from the left side.
func (db *DB) LPop(key string, count int64) ([]string, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LPop(key, count)
	}
	return db.popList(key, ListLeft, count)
}

// RPop removes and returns up to count elements from the right side.
func (db *DB) RPop(key string, count int64) ([]string, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).RPop(key, count)
	}
	return db.popList(key, ListRight, count)
}

func (db *DB) popList(key string, direction ListDirection, count int64) ([]string, error) {
	if !validListDirection(direction) {
		return nil, ErrInvalidDirection
	}
	if count < 0 {
		return nil, ErrInvalidInteger
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return []string{}, nil
	}
	if e.kind != KindList {
		return nil, ErrWrongType
	}
	if count == 0 {
		return []string{}, nil
	}

	list := e.value.(*deque)
	take := list.len()
	if count < int64(take) {
		take = int(count)
	}
	values := make([]string, take)
	for i := range values {
		if direction == ListLeft {
			values[i] = list.popLeft()
		} else {
			values[i] = list.popRight()
		}
	}
	if list.len() == 0 {
		delete(db.entries, key)
	}
	return values, nil
}

// LLen returns the number of elements in the list at key.
func (db *DB) LLen(key string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LLen(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindList {
		return 0, ErrWrongType
	}
	return e.value.(*deque).len(), nil
}

// LRange returns elements between the inclusive start and stop indexes.
// Negative indexes count backward from the right side.
func (db *DB) LRange(key string, start, stop int64) ([]string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LRange(key, start, stop)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return []string{}, nil
	}
	if e.kind != KindList {
		return nil, ErrWrongType
	}

	list := e.value.(*deque)
	first, last, ok := normalizeListRange(start, stop, list.len())
	if !ok {
		return []string{}, nil
	}
	values := make([]string, last-first+1)
	for i := range values {
		values[i] = list.at(first + i)
	}
	return values, nil
}

// LIndex returns the element at index. Negative indexes count backward from
// the right side.
func (db *DB) LIndex(key string, index int64) (string, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LIndex(key, index)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != KindList {
		return "", false, ErrWrongType
	}

	list := e.value.(*deque)
	normalized, ok := normalizeListIndex(index, list.len())
	if !ok {
		return "", false, nil
	}
	return list.at(normalized), true, nil
}

// LSet replaces the element at index.
func (db *DB) LSet(key string, index int64, value string) error {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LSet(key, index, value)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return ErrIndexOutOfRange
	}
	if e.kind != KindList {
		return ErrWrongType
	}

	list := e.value.(*deque)
	normalized, ok := normalizeListIndex(index, list.len())
	if !ok {
		return ErrIndexOutOfRange
	}
	list.set(normalized, value)
	return nil
}

// LTrim keeps only elements between the inclusive start and stop indexes.
func (db *DB) LTrim(key string, start, stop int64) error {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LTrim(key, start, stop)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return nil
	}
	if e.kind != KindList {
		return ErrWrongType
	}

	list := e.value.(*deque)
	first, last, ok := normalizeListRange(start, stop, list.len())
	if !ok {
		delete(db.entries, key)
		return nil
	}
	values := make([]string, last-first+1)
	for i := range values {
		values[i] = list.at(first + i)
	}
	list.replace(values)
	return nil
}

// LInsert inserts value before or after the first pivot. It returns the new
// length, 0 for a missing key, or -1 when the pivot is absent.
func (db *DB) LInsert(key string, before bool, pivot, value string) (int, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LInsert(key, before, pivot, value)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindList {
		return 0, ErrWrongType
	}

	list := e.value.(*deque)
	for i := 0; i < list.len(); i++ {
		if list.at(i) != pivot {
			continue
		}
		index := i
		if !before {
			index++
		}
		list.insert(index, value)
		return list.len(), nil
	}
	return -1, nil
}

// LRem removes occurrences of value according to count. Positive counts scan
// from the left, negative counts scan from the right, and zero removes all.
func (db *DB) LRem(key string, count int64, value string) (int, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		defer release()
		return db.shardFor(key).LRem(key, count, value)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindList {
		return 0, ErrWrongType
	}

	list := e.value.(*deque)
	remove := make([]bool, list.len())
	removed := 0
	if count >= 0 {
		for i := 0; i < list.len(); i++ {
			if list.at(i) != value || count > 0 && int64(removed) >= count {
				continue
			}
			remove[i] = true
			removed++
		}
	} else {
		remaining := count
		for i := list.len() - 1; i >= 0 && remaining < 0; i-- {
			if list.at(i) != value {
				continue
			}
			remove[i] = true
			removed++
			remaining++
		}
	}
	if removed == 0 {
		return 0, nil
	}

	values := make([]string, 0, list.len()-removed)
	for i := 0; i < list.len(); i++ {
		if !remove[i] {
			values = append(values, list.at(i))
		}
	}
	if len(values) == 0 {
		delete(db.entries, key)
	} else {
		list.replace(values)
	}
	return removed, nil
}

// LMove atomically pops one element from source and pushes it to destination.
func (db *DB) LMove(
	source, destination string,
	from, to ListDirection,
) (string, bool, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		var value string
		var found bool
		var err error
		db.withView([]string{source, destination}, func(view *DB) { value, found, err = view.LMove(source, destination, from, to) })
		if found {
			db.serveShardedWaitersLocked(destination)
		}
		return value, found, err
	}
	if !validListDirection(from) || !validListDirection(to) {
		return "", false, ErrInvalidDirection
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	value, found, err := db.moveListLocked(source, destination, from, to)
	if err != nil || !found {
		return value, found, err
	}
	db.serveListWaitersLocked(destination)
	return value, true, nil
}

func (db *DB) moveListLocked(
	source, destination string,
	from, to ListDirection,
) (string, bool, error) {

	sourceEntry, sourceExists := db.entries[source]
	if sourceExists && sourceEntry.kind != KindList {
		return "", false, ErrWrongType
	}
	destinationEntry, destinationExists := db.entries[destination]
	if destinationExists && destinationEntry.kind != KindList {
		return "", false, ErrWrongType
	}
	if !sourceExists {
		return "", false, nil
	}

	sourceList := sourceEntry.value.(*deque)
	var value string
	if from == ListLeft {
		value = sourceList.popLeft()
	} else {
		value = sourceList.popRight()
	}

	if source == destination {
		destinationEntry = sourceEntry
	} else {
		if sourceList.len() == 0 {
			delete(db.entries, source)
		}
		if !destinationExists {
			destinationEntry = &entry{kind: KindList, value: newDeque()}
			db.entries[destination] = destinationEntry
		}
	}

	destinationList := destinationEntry.value.(*deque)
	if to == ListLeft {
		destinationList.pushLeft(value)
	} else {
		destinationList.pushRight(value)
	}
	return value, true, nil
}

// RPopLPush is the legacy equivalent of LMOVE RIGHT LEFT.
func (db *DB) RPopLPush(source, destination string) (string, bool, error) {
	if db.shards != nil {
		return db.LMove(source, destination, ListRight, ListLeft)
	}
	return db.LMove(source, destination, ListRight, ListLeft)
}

func normalizeListIndex(index int64, length int) (int, bool) {
	if index < 0 {
		index += int64(length)
	}
	if index < 0 || index >= int64(length) {
		return 0, false
	}
	return int(index), true
}

func normalizeListRange(start, stop int64, length int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}
	listLength := int64(length)
	if start < 0 {
		start += listLength
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += listLength
	}
	if start >= listLength || stop < 0 || start > stop {
		return 0, 0, false
	}
	if stop >= listLength {
		stop = listLength - 1
	}
	return int(start), int(stop), true
}

func validListDirection(direction ListDirection) bool {
	return direction == ListLeft || direction == ListRight
}
