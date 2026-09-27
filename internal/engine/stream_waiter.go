package engine

import "context"

type streamWaitState uint8

const (
	streamWaiting streamWaitState = iota
	streamWaitComplete
	streamWaitCanceled
)

type streamWaiter struct {
	requests []StreamReadRequest
	count    int64
	group    string
	consumer string
	noAck    bool
	resolved bool
	state    streamWaitState
	result   chan streamWaitResult
}

type streamWaitResult struct {
	streams []StreamReadResult
	err     error
}

func (db *DB) WaitForStreamRead(ctx context.Context, requests []StreamReadRequest, count int64) ([]StreamReadResult, error) {
	if db.shards == nil {
		return db.XRead(requests, count)
	}
	if len(requests) == 0 {
		return nil, ErrNoKeys
	}
	waiter := &streamWaiter{
		requests: append([]StreamReadRequest(nil), requests...),
		count:    count,
		state:    streamWaiting,
		result:   make(chan streamWaitResult, 1),
	}
	return db.startAndAwaitStreamWaiter(ctx, waiter)
}

func (db *DB) WaitForStreamGroupRead(ctx context.Context, group, consumer string, requests []StreamReadRequest, options StreamGroupReadOptions) ([]StreamReadResult, error) {
	if db.shards == nil {
		return db.XReadGroup(group, consumer, requests, options)
	}
	waiter := &streamWaiter{
		requests: append([]StreamReadRequest(nil), requests...),
		count:    options.Count,
		group:    group,
		consumer: consumer,
		noAck:    options.NoAck,
		resolved: true,
		state:    streamWaiting,
		result:   make(chan streamWaitResult, 1),
	}
	return db.startAndAwaitStreamWaiter(ctx, waiter)
}

func (db *DB) startAndAwaitStreamWaiter(ctx context.Context, waiter *streamWaiter) ([]StreamReadResult, error) {
	db.waitMu.Lock()
	result, ready := db.tryStreamWaiterLocked(waiter)
	if ready {
		db.waitMu.Unlock()
		return result.streams, result.err
	}
	if db.waitersClosed {
		db.waitMu.Unlock()
		return nil, context.Canceled
	}
	db.registerStreamWaiterLocked(waiter)
	db.waitMu.Unlock()

	select {
	case result := <-waiter.result:
		return result.streams, result.err
	case <-ctx.Done():
		db.waitMu.Lock()
		if waiter.state == streamWaiting {
			waiter.state = streamWaitCanceled
			db.removeStreamWaiterLocked(waiter)
			db.waitMu.Unlock()
			return nil, ctx.Err()
		}
		db.waitMu.Unlock()
		result := <-waiter.result
		return result.streams, result.err
	}
}

func (db *DB) tryStreamWaiterLocked(waiter *streamWaiter) (streamWaitResult, bool) {
	keys := streamRequestKeys(waiter.requests)
	var streams []StreamReadResult
	var err error
	db.withView(keys, func(view *DB) {
		if !waiter.resolved {
			for index := range waiter.requests {
				if !waiter.requests[index].UseLatest {
					continue
				}
				id, _, idErr := view.XLastID(waiter.requests[index].Key)
				if idErr != nil {
					err = idErr
					return
				}
				waiter.requests[index].After = id
				waiter.requests[index].UseLatest = false
			}
			waiter.resolved = true
		}
		if waiter.group == "" {
			streams, err = view.XRead(waiter.requests, waiter.count)
			return
		}
		streams, err = view.XReadGroup(waiter.group, waiter.consumer, waiter.requests, StreamGroupReadOptions{Count: waiter.count, NoAck: waiter.noAck})
	})
	return streamWaitResult{streams: streams, err: err}, err != nil || len(streams) > 0
}

func (db *DB) registerStreamWaiterLocked(waiter *streamWaiter) {
	seen := make(map[string]struct{}, len(waiter.requests))
	for _, request := range waiter.requests {
		if _, exists := seen[request.Key]; exists {
			continue
		}
		seen[request.Key] = struct{}{}
		db.streamWaiters[request.Key] = append(db.streamWaiters[request.Key], waiter)
	}
}

func (db *DB) removeStreamWaiterLocked(waiter *streamWaiter) {
	seen := make(map[string]struct{}, len(waiter.requests))
	for _, request := range waiter.requests {
		if _, exists := seen[request.Key]; exists {
			continue
		}
		seen[request.Key] = struct{}{}
		queue := db.streamWaiters[request.Key]
		for index, queued := range queue {
			if queued != waiter {
				continue
			}
			copy(queue[index:], queue[index+1:])
			queue[len(queue)-1] = nil
			queue = queue[:len(queue)-1]
			break
		}
		if len(queue) == 0 {
			delete(db.streamWaiters, request.Key)
		} else {
			db.streamWaiters[request.Key] = queue
		}
	}
}

// Caller holds waitMu. Non-group readers observe the same append, while group
// readers compete through the group's atomic pending-entry assignment.
func (db *DB) serveStreamWaitersLocked(keys ...string) {
	seen := make(map[*streamWaiter]struct{})
	ordered := make([]*streamWaiter, 0)
	for _, key := range keys {
		for _, waiter := range db.streamWaiters[key] {
			if _, exists := seen[waiter]; exists {
				continue
			}
			seen[waiter] = struct{}{}
			ordered = append(ordered, waiter)
		}
	}
	for _, waiter := range ordered {
		if waiter.state != streamWaiting {
			continue
		}
		result, ready := db.tryStreamWaiterLocked(waiter)
		if ready {
			db.completeStreamWaiterLocked(waiter, result)
		}
	}
}

func (db *DB) completeStreamWaiterLocked(waiter *streamWaiter, result streamWaitResult) {
	if waiter.state != streamWaiting {
		return
	}
	waiter.state = streamWaitComplete
	db.removeStreamWaiterLocked(waiter)
	waiter.result <- result
}
