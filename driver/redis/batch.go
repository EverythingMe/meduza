package redis

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// Batch represents a set of batched results, either a transaction or just send/receive.
//
// It has two key features:
//
// 1. Automating the send/flush/receive logic, and replacing it with a single Execute() call
// that takes care of retrieving the right number of values
//
// 2. To ease the extraction of return values, it returns a "promise" from each Send call.
// After Execute() is called, these promises are filled with the return value of executing
// the queued command
type Batch struct {
	conn     redis.Conn
	count    int
	promises []*Promise
}

// A transaction is similar to a Batch, but has MULTI/EXEC/DISCARD
type Transaction struct {
	*Batch
	initialized bool
}

// Promise represents the future return value of a queued commmand in a batch.
//
// Each call to Send returns a promise that will be filled with the result of
// executing the sent command after Execute is called.
type Promise struct {
	Value interface{}
}

// Reply returns the underlying reply value of this promise, after executing the batch.
// It is nil before that. We also return a nil error to be able to use redigo's
// automatic conversion utility functions. If you just need the value you can access it directly
func (p *Promise) Reply() (interface{}, error) {
	return p.Value, nil
}

func (p *Promise) String() string {
	return fmt.Sprintf("%v", p.Value)
}

// NewBatch create a new non-transactional batch object
func NewBatch(conn redis.Conn) *Batch {
	return &Batch{
		conn:     conn,
		promises: make([]*Promise, 0),
	}

}

// NewBatchTransaction creates a new transactional batch object
func NewTransaction(conn redis.Conn) *Transaction {

	ret := &Transaction{
		&Batch{conn: conn}, false,
	}

	return ret
}

// reset the batch after execute/abort so it can be re-used
func (b *Batch) reset() {
	b.promises = make([]*Promise, 0)
	b.count = 0

}

// reset resets the transaction
func (t *Transaction) reset() {
	t.Batch.reset()
	t.initialized = false

}

// Send enqueues a single command in the batch, and returns a promise that will contain
// the return value of executing this command when Execute is called.
func (b *Batch) Send(commandName string, args ...interface{}) (*Promise, error) {

	err := b.conn.Send(commandName, args...)

	if err == nil {

		b.count++
		promise := new(Promise)
		b.promises = append(b.promises, promise)

		return promise, nil
	}

	return nil, err
}

// Abort drains the receive buffer and closes the connection. It doesn't return an error as it is
// meant to be a deferred call
func (b *Batch) Abort() {

	defer b.reset()
	defer b.conn.Close()
	if b.count == 0 {
		return
	}

	if err := b.conn.Flush(); err != nil {
		return
	}

	// drain the connection of any pending unreceived messages if it was aborted in the middle
	for i := 0; i < b.count; i++ {

		if _, err := b.conn.Receive(); err != nil {
			return
		}

	}

}

// Send enqueues a single command in the batch, and returns a promise that will contain
// the return value of executing this command when Execute is called.
func (b *Transaction) Send(commandName string, args ...interface{}) (*Promise, error) {

	//if the transaction is not initialized, we initialize it and send a MULTI command
	if !b.initialized {
		if err := b.conn.Send("MULTI"); err != nil {
			return nil, err
		}
		b.initialized = true
	}
	return b.Batch.Send(commandName, args...)
}

// Abort aborts a transaction batch by calling redis ABORT.
// Note that it will keep all promises empty and reset the transaction automatically
func (b *Transaction) Abort() error {
	defer b.conn.Close()
	defer b.reset()

	if b.initialized {
		if _, err := b.conn.Do("DISCARD"); err != nil {
			return err
		}

	}

	return nil

}

// Execute flushes the connection and executes the batch if it's a transaction, and
// automatically receives all the return values, setting them in the previously returned promises.
//
// We also return all the promises in order if you're just interested in them after execution
func (b *Batch) Execute() ([]*Promise, error) {

	defer b.reset()

	if err := b.conn.Flush(); err != nil {
		b.conn.Close()
		return nil, err
	}

	// assign the respective return values to all promises
	for i := 0; i < b.count; i++ {

		reply, err := b.conn.Receive()

		if err != nil {
			b.conn.Close()
			return nil, err
		}

		b.promises[i].Value = reply
	}

	return b.promises, nil

}

// Execute a transaction
func (t *Transaction) Execute() ([]*Promise, error) {

	defer t.reset()

	// nothing was sent to this tx, just leave it
	if !t.initialized {
		return t.promises, nil
	}

	if results, err := redis.Values(t.conn.Do("EXEC")); err != nil {
		t.conn.Close()
		return nil, err
	} else {
		// assign the return values to all promises
		for i, v := range results {
			t.promises[i].Value = v
		}
	}

	return t.promises, nil

}
