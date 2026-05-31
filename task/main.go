package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

type Queue struct {
	messages []string
	waiters  []*waiter
}

type waiter struct {
	ch chan string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "port is required")
		os.Exit(1)
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil || port <= 0 || port > 65535 {
		fmt.Fprintln(os.Stderr, "invalid port")
		os.Exit(1)
	}

	broker := &Broker{queues: make(map[string]*Queue)}
	addr := ":" + strconv.Itoa(port)
	if err := http.ListenAndServe(addr, broker); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	if queueName == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		b.handlePut(w, r, queueName)
	case http.MethodGet:
		b.handleGet(w, r, queueName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handlePut(w http.ResponseWriter, r *http.Request, queueName string) {
	values, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	msg := ""
	if len(values) > 0 {
		msg = values[0]
	}

	var target *waiter

	b.mu.Lock()
	q := b.getOrCreateQueue(queueName)
	if len(q.waiters) > 0 {
		target = q.waiters[0]
		q.waiters = q.waiters[1:]
	} else {
		q.messages = append(q.messages, msg)
	}
	b.mu.Unlock()

	if target != nil {
		target.ch <- msg
	}

	w.WriteHeader(http.StatusOK)
}

func (b *Broker) handleGet(w http.ResponseWriter, r *http.Request, queueName string) {
	timeout, hasTimeout, badTimeout := parseTimeout(r)
	if badTimeout {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	b.mu.Lock()
	q := b.queues[queueName]
	if q != nil && len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, msg)
		return
	}

	if !hasTimeout || timeout == 0 {
		b.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if q == nil {
		q = b.getOrCreateQueue(queueName)
	}

	wt := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, wt)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-wt.ch:
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, msg)
	case <-timer.C:
		b.mu.Lock()
		removed := removeWaiter(q, wt)
		b.mu.Unlock()
		if removed {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		msg := <-wt.ch
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, msg)
	case <-r.Context().Done():
		b.mu.Lock()
		_ = removeWaiter(q, wt)
		b.mu.Unlock()
	}
}

func parseTimeout(r *http.Request) (time.Duration, bool, bool) {
	values, ok := r.URL.Query()["timeout"]
	if !ok {
		return 0, false, false
	}

	raw := ""
	if len(values) > 0 {
		raw = values[0]
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, false, true
	}

	return time.Duration(n) * time.Second, true, false
}

func (b *Broker) getOrCreateQueue(name string) *Queue {
	q := b.queues[name]
	if q == nil {
		q = &Queue{}
		b.queues[name] = q
	}
	return q
}

func removeWaiter(q *Queue, w *waiter) bool {
	for i := range q.waiters {
		if q.waiters[i] == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return true
		}
	}
	return false
}
