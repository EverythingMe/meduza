package query

import (
	"fmt"
	"time"

	"github.com/EverythingMe/meduza/errors"
)

// Query is just the commmon interface for all queries.
// They must validate themselves
type Query interface {
	Validate() error
}

type Ordering struct {
	By        string `bson:"by"`
	Ascending bool   `bson:"asc"`
}

var NoOrder = Ordering{Ascending: true}

// IsNil tells us whether an Ordering object actually contains any ordering directive or is just empty
func (o Ordering) IsNil() bool {
	return len(o.By) == 0
}

func (o Ordering) Validate() error {
	return nil
}

type Paging struct {
	Offset int `bson:"offset"`
	Limit  int `bson:"limit"`
}

func (p Paging) Validate() error {

	switch {
	case p.Offset < 0:
		fallthrough
	case p.Limit == 0:
		fallthrough
	case p.Limit < -1:
		return errors.NewError("Invalid paging parameters: offset: %d, limit: %d", p.Offset, p.Limit)
	default:
		return nil
	}

}

// This sets the deault limit for queries that don't have limit built into them
var DefaultPagingLimit = 100

type Response struct {
	Time      time.Duration `bson:"time"`
	startTime time.Time
	Error     *errors.Error `bson:"error"`
}

type QueryResult interface {
	Elapsed() time.Duration
	Err() error
}

func NewResponse(err error) *Response {

	return &Response{
		startTime: time.Now(),
		Error:     errors.Wrap(err),
	}
}

func (r *Response) Elapsed() time.Duration {
	return r.Time
}

func (r *Response) Err() error {
	if r.Error == nil {
		return nil
	}
	return r.Error
}

func (r *Response) Done() {
	r.Time = time.Since(r.startTime)
}

func (r *Response) String() string {
	return fmt.Sprintf("Response: {Time: %v, Error: %v}", r.Time, r.Error)
}

type PingQuery struct{}

func (PingQuery) Validate() error {
	return nil
}

type PingResponse struct {
	*Response
}

func NewPingResponse() PingResponse {
	return PingResponse{
		NewResponse(nil),
	}
}
