package engine

import "context"

type listWaitKind uint8

const (
	listWaitPop listWaitKind = iota
	listWaitMove
)

type listWaitState uint8

const (
	listWaiting listWaitState = iota
	listWaitComplete
	listWaitCanceled
)

type listWaiter struct {
	kind        listWaitKind
	keys        []string
	destination string
	from        ListDirection
	to          ListDirection
	state       listWaitState
	result      chan listWaitResult
}

type listWaitResult struct {
	key   string
	value string
	found bool
	err   error
}

// WaitForListPop returns immediately from the first non-empty key or waits
// until a list becomes ready or ctx is canceled. Keys retain argument order.
func (db *DB) WaitForListPop(
	ctx context.Context,
	keys []string,
	direction ListDirection,
) (string, string, bool, error) {
	if db.shards != nil {
		return db.waitForShardedListPop(ctx, keys, direction)
	}
	if !validListDirection(direction) {
		return "", "", false, ErrInvalidDirection
	}
	if len(keys) == 0 {
		return "", "", false, ErrNoKeys
	}

	waiter := &listWaiter{
		kind:   listWaitPop,
		keys:   append([]string(nil), keys...),
		from:   direction,
		state:  listWaiting,
		result: make(chan listWaitResult, 1),
	}
	result, wait := db.startListWait(waiter)
	if !wait {
		return result.key, result.value, result.found, result.err
	}
	result, err := db.awaitListWaiter(ctx, waiter)
	if err != nil {
		return "", "", false, err
	}
	return result.key, result.value, result.found, result.err
}

// WaitForListMove behaves like LMove when source is non-empty and otherwise
// waits until an element can be moved or ctx is canceled.
func (db *DB) WaitForListMove(
	ctx context.Context,
	source, destination string,
	from, to ListDirection,
) (string, bool, error) {
	if db.shards != nil {
		return db.waitForShardedListMove(ctx, source, destination, from, to)
	}
	if !validListDirection(from) || !validListDirection(to) {
		return "", false, ErrInvalidDirection
	}

	waiter := &listWaiter{
		kind:        listWaitMove,
		keys:        []string{source},
		destination: destination,
		from:        from,
		to:          to,
		state:       listWaiting,
		result:      make(chan listWaitResult, 1),
	}
	result, wait := db.startListWait(waiter)
	if !wait {
		return result.value, result.found, result.err
	}
	result, err := db.awaitListWaiter(ctx, waiter)
	if err != nil {
		return "", false, err
	}
	return result.value, result.found, result.err
}

func (db *DB) startListWait(waiter *listWaiter) (listWaitResult, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	result, ready, destinationReady := db.tryListWaiterLocked(waiter)
	if ready {
		if destinationReady != "" {
			db.serveListWaitersLocked(destinationReady)
		}
		return result, false
	}
	if db.waitersClosed {
		return listWaitResult{err: context.Canceled}, false
	}
	db.registerListWaiterLocked(waiter)
	return listWaitResult{}, true
}

func (db *DB) awaitListWaiter(ctx context.Context, waiter *listWaiter) (listWaitResult, error) {
	select {
	case result := <-waiter.result:
		return result, nil
	case <-ctx.Done():
		db.mu.Lock()
		if waiter.state == listWaiting {
			waiter.state = listWaitCanceled
			db.removeListWaiterLocked(waiter)
			db.mu.Unlock()
			return listWaitResult{}, ctx.Err()
		}
		db.mu.Unlock()
		return <-waiter.result, nil
	}
}

func (db *DB) registerListWaiterLocked(waiter *listWaiter) {
	seen := make(map[string]struct{}, len(waiter.keys))
	for _, key := range waiter.keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		db.listWaiters[key] = append(db.listWaiters[key], waiter)
	}
}

func (db *DB) removeListWaiterLocked(waiter *listWaiter) {
	seen := make(map[string]struct{}, len(waiter.keys))
	for _, key := range waiter.keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		queue := db.listWaiters[key]
		for i, queued := range queue {
			if queued != waiter {
				continue
			}
			copy(queue[i:], queue[i+1:])
			queue[len(queue)-1] = nil
			queue = queue[:len(queue)-1]
			break
		}
		if len(queue) == 0 {
			delete(db.listWaiters, key)
		} else {
			db.listWaiters[key] = queue
		}
	}
}

func (db *DB) serveListWaitersLocked(keys ...string) {
	pending := append([]string(nil), keys...)
	for len(pending) > 0 {
		key := pending[0]
		pending = pending[1:]

		for {
			queue := db.listWaiters[key]
			for len(queue) > 0 && queue[0].state != listWaiting {
				queue[0] = nil
				queue = queue[1:]
			}
			if len(queue) == 0 {
				delete(db.listWaiters, key)
				break
			}
			db.listWaiters[key] = queue

			result, ready, destinationReady := db.tryListWaiterLocked(queue[0])
			if !ready {
				break
			}
			db.completeListWaiterLocked(queue[0], result)
			if destinationReady != "" {
				pending = append(pending, destinationReady)
			}
		}
	}
}

func (db *DB) tryListWaiterLocked(waiter *listWaiter) (listWaitResult, bool, string) {
	if waiter.kind == listWaitMove {
		value, found, err := db.moveListLocked(
			waiter.keys[0], waiter.destination, waiter.from, waiter.to,
		)
		if err != nil {
			return listWaitResult{err: err}, true, ""
		}
		if !found {
			return listWaitResult{}, false, ""
		}
		return listWaitResult{value: value, found: true}, true, waiter.destination
	}

	for _, key := range waiter.keys {
		e, ok := db.entries[key]
		if !ok {
			continue
		}
		if e.kind != KindList {
			return listWaitResult{err: ErrWrongType}, true, ""
		}
		list := e.value.(*deque)
		var value string
		if waiter.from == ListLeft {
			value = list.popLeft()
		} else {
			value = list.popRight()
		}
		if list.len() == 0 {
			delete(db.entries, key)
		}
		return listWaitResult{key: key, value: value, found: true}, true, ""
	}
	return listWaitResult{}, false, ""
}

func (db *DB) completeListWaiterLocked(waiter *listWaiter, result listWaitResult) {
	if waiter.state != listWaiting {
		return
	}
	waiter.state = listWaitComplete
	db.removeListWaiterLocked(waiter)
	waiter.result <- result
}

// CloseWaiters cancels current and future blocking operations. It is intended
// for server shutdown.
func (db *DB) CloseWaiters() {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		db.waitersClosed = true
		unique := make(map[*listWaiter]struct{})
		for _, queue := range db.listWaiters {
			for _, waiter := range queue {
				unique[waiter] = struct{}{}
			}
		}
		for waiter := range unique {
			db.completeListWaiterLocked(waiter, listWaitResult{err: context.Canceled})
		}
		streamUnique := make(map[*streamWaiter]struct{})
		for _, queue := range db.streamWaiters {
			for _, waiter := range queue {
				streamUnique[waiter] = struct{}{}
			}
		}
		for waiter := range streamUnique {
			db.completeStreamWaiterLocked(waiter, streamWaitResult{err: context.Canceled})
		}
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	db.waitersClosed = true
	unique := make(map[*listWaiter]struct{})
	for _, queue := range db.listWaiters {
		for _, waiter := range queue {
			unique[waiter] = struct{}{}
		}
	}
	for waiter := range unique {
		db.completeListWaiterLocked(waiter, listWaitResult{err: context.Canceled})
	}
	streamUnique := make(map[*streamWaiter]struct{})
	for _, queue := range db.streamWaiters {
		for _, waiter := range queue {
			streamUnique[waiter] = struct{}{}
		}
	}
	for waiter := range streamUnique {
		db.completeStreamWaiterLocked(waiter, streamWaitResult{err: context.Canceled})
	}
}
