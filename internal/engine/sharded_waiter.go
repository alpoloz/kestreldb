package engine

import "context"

func (db *DB) waitForShardedListPop(ctx context.Context, keys []string, direction ListDirection) (string, string, bool, error) {
	if !validListDirection(direction) {
		return "", "", false, ErrInvalidDirection
	}
	if len(keys) == 0 {
		return "", "", false, ErrNoKeys
	}
	waiter := &listWaiter{kind: listWaitPop, keys: append([]string(nil), keys...), from: direction, state: listWaiting, result: make(chan listWaitResult, 1)}
	result, wait := db.startShardedListWait(waiter)
	if !wait {
		return result.key, result.value, result.found, result.err
	}
	result, err := db.awaitShardedListWait(ctx, waiter)
	if err != nil {
		return "", "", false, err
	}
	return result.key, result.value, result.found, result.err
}

func (db *DB) waitForShardedListMove(ctx context.Context, source, destination string, from, to ListDirection) (string, bool, error) {
	if !validListDirection(from) || !validListDirection(to) {
		return "", false, ErrInvalidDirection
	}
	waiter := &listWaiter{kind: listWaitMove, keys: []string{source}, destination: destination, from: from, to: to, state: listWaiting, result: make(chan listWaitResult, 1)}
	result, wait := db.startShardedListWait(waiter)
	if !wait {
		return result.value, result.found, result.err
	}
	result, err := db.awaitShardedListWait(ctx, waiter)
	if err != nil {
		return "", false, err
	}
	return result.value, result.found, result.err
}

func (db *DB) startShardedListWait(waiter *listWaiter) (listWaitResult, bool) {
	db.waitMu.Lock()
	defer db.waitMu.Unlock()
	result, ready, destinationReady := db.tryShardedListWaiter(waiter)
	if ready {
		if destinationReady != "" {
			db.serveShardedWaitersLocked(destinationReady)
		}
		return result, false
	}
	if db.waitersClosed {
		return listWaitResult{err: context.Canceled}, false
	}
	db.registerListWaiterLocked(waiter)
	return listWaitResult{}, true
}

func (db *DB) awaitShardedListWait(ctx context.Context, waiter *listWaiter) (listWaitResult, error) {
	select {
	case result := <-waiter.result:
		return result, nil
	case <-ctx.Done():
		db.waitMu.Lock()
		if waiter.state == listWaiting {
			waiter.state = listWaitCanceled
			db.removeListWaiterLocked(waiter)
			db.waitMu.Unlock()
			return listWaitResult{}, ctx.Err()
		}
		db.waitMu.Unlock()
		return <-waiter.result, nil
	}
}

func (db *DB) tryShardedListWaiter(waiter *listWaiter) (listWaitResult, bool, string) {
	keys := append([]string(nil), waiter.keys...)
	if waiter.kind == listWaitMove {
		keys = append(keys, waiter.destination)
	}
	var result listWaitResult
	var ready bool
	var destinationReady string
	db.withView(keys, func(view *DB) { result, ready, destinationReady = view.tryListWaiterLocked(waiter) })
	return result, ready, destinationReady
}

// Caller holds waitMu. A pushed element is offered to the oldest eligible
// waiter before another list command can consume it.
func (db *DB) serveShardedWaitersLocked(keys ...string) {
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
			result, ready, destinationReady := db.tryShardedListWaiter(queue[0])
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
