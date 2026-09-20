package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

type popResult struct {
	key   string
	value string
	found bool
	err   error
}

func TestWaitForListPopImmediateAndWake(t *testing.T) {
	db := NewDB()
	if _, err := db.RPush("ready", "value"); err != nil {
		t.Fatal(err)
	}

	key, value, found, err := db.WaitForListPop(context.Background(), []string{"missing", "ready"}, ListLeft)
	if err != nil || !found || key != "ready" || value != "value" {
		t.Fatalf("immediate pop = (%q, %q, %t, %v)", key, value, found, err)
	}

	result := make(chan popResult, 1)
	go func() {
		key, value, found, err := db.WaitForListPop(context.Background(), []string{"later"}, ListLeft)
		result <- popResult{key: key, value: value, found: found, err: err}
	}()
	waitForListWaiters(t, db, "later", 1)

	if length, err := db.RPush("later", "first", "second"); err != nil || length != 2 {
		t.Fatalf("RPush() = (%d, %v), want (2, nil)", length, err)
	}
	got := <-result
	if got.err != nil || !got.found || got.key != "later" || got.value != "first" {
		t.Fatalf("waited pop = %#v", got)
	}
	assertListRange(t, db, "later", 0, -1, "second")
}

func TestListWaitersPreserveKeyAndClientOrder(t *testing.T) {
	db := NewDB()
	firstResult := make(chan popResult, 1)
	secondResult := make(chan popResult, 1)

	go func() {
		key, value, found, err := db.WaitForListPop(context.Background(), []string{"first", "second"}, ListLeft)
		firstResult <- popResult{key: key, value: value, found: found, err: err}
	}()
	waitForListWaiters(t, db, "first", 1)

	go func() {
		key, value, found, err := db.WaitForListPop(context.Background(), []string{"first"}, ListLeft)
		secondResult <- popResult{key: key, value: value, found: found, err: err}
	}()
	waitForListWaiters(t, db, "first", 2)

	db.waitMu.Lock()
	db.withView([]string{"first", "second"}, func(view *DB) {
		view.entries["first"] = &entry{kind: KindList, value: newDeque("from-first", "for-second-waiter")}
		view.entries["second"] = &entry{kind: KindList, value: newDeque("from-second")}
	})
	db.serveShardedWaitersLocked("second", "first")
	db.waitMu.Unlock()

	first := <-firstResult
	if first.err != nil || first.key != "first" || first.value != "from-first" {
		t.Fatalf("first waiter = %#v", first)
	}
	second := <-secondResult
	if second.err != nil || second.key != "first" || second.value != "for-second-waiter" {
		t.Fatalf("second waiter = %#v", second)
	}
	assertListRange(t, db, "second", 0, -1, "from-second")
}

func TestListWaiterCancellationDoesNotConsume(t *testing.T) {
	db := NewDB()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, _, err := db.WaitForListPop(ctx, []string{"list"}, ListLeft)
		result <- err
	}()
	waitForListWaiters(t, db, "list", 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}

	if _, err := db.RPush("list", "value"); err != nil {
		t.Fatal(err)
	}
	assertListRange(t, db, "list", 0, -1, "value")
}

func TestWaitForListMoveAndDestinationError(t *testing.T) {
	db := NewDB()
	result := make(chan popResult, 1)
	go func() {
		value, found, err := db.WaitForListMove(
			context.Background(), "source", "destination", ListRight, ListLeft,
		)
		result <- popResult{value: value, found: found, err: err}
	}()
	waitForListWaiters(t, db, "source", 1)

	if _, err := db.RPush("source", "value"); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil || !got.found || got.value != "value" {
		t.Fatalf("waited move = %#v", got)
	}
	assertListRange(t, db, "destination", 0, -1, "value")

	errorResult := make(chan error, 1)
	go func() {
		_, _, err := db.WaitForListMove(
			context.Background(), "error-source", "error-destination", ListLeft, ListRight,
		)
		errorResult <- err
	}()
	waitForListWaiters(t, db, "error-source", 1)
	db.Set("error-destination", []byte("wrong type"))
	if _, err := db.RPush("error-source", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := <-errorResult; !errors.Is(err, ErrWrongType) {
		t.Fatalf("waited move error = %v, want ErrWrongType", err)
	}
	assertListRange(t, db, "error-source", 0, -1, "retained")
}

func TestCloseWaiters(t *testing.T) {
	db := NewDB()
	result := make(chan error, 1)
	go func() {
		_, _, _, err := db.WaitForListPop(context.Background(), []string{"list"}, ListLeft)
		result <- err
	}()
	waitForListWaiters(t, db, "list", 1)

	db.CloseWaiters()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}
	if _, _, _, err := db.WaitForListPop(context.Background(), []string{"other"}, ListLeft); !errors.Is(err, context.Canceled) {
		t.Fatalf("future wait error = %v, want context.Canceled", err)
	}
}

func waitForListWaiters(t *testing.T, db *DB, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if db.shards != nil {
			db.waitMu.Lock()
		} else {
			db.mu.RLock()
		}
		got := len(db.listWaiters[key])
		if db.shards != nil {
			db.waitMu.Unlock()
		} else {
			db.mu.RUnlock()
		}
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiters for %q did not reach %d", key, want)
}
