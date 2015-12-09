package client

import (
	"sync"

	"github.com/EverythingMe/meduza/errors"
)

type Client interface {
	Do(query interface{}) (interface{}, error)
	//Pipeline() Pipeline
}

// Pipeline is a TBD interface for allowing MULTI transactions
type Pipeline interface {
	Send(query interface{}) error
	Abort() error
	Execute() ([]interface{}, error)
}

type Dialer interface {
	Dial() (Client, error)
}

type Pool struct {
	pool *sync.Pool
}

func NewPool(dialer Dialer) *Pool {

	return &Pool{
		pool: &sync.Pool{
			New: func() interface{} {
				if cl, err := dialer.Dial(); err == nil {
					return cl
				}
				return nil
			},
		},
	}
}

func (p *Pool) Get() (Client, error) {
	c := p.pool.Get()
	if c == nil {
		return nil, errors.NewError("Could not connect to server")
	}
	return c.(Client), nil
}

func (p *Pool) Return(c Client) {
	p.pool.Put(c)
}
