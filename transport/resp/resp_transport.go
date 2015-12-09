package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/transport"
)

type Transport struct {
	conn   io.ReadWriter
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewTransport(conn io.ReadWriter) *Transport {
	return &Transport{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (r *Transport) ReadMessage() (transport.Message, error) {
	//read an parse the request
	cmd, err := read(r.reader)

	if err != nil {
		return transport.Message{}, err
	}

	logging.Info("Read %d bytes from %v", len(cmd.Key), r.conn)
	return transport.Message{
		Type: transport.MessageType(bytes.ToUpper(cmd.Command)),
		Body: cmd.Key,
	}, nil

}

func writeBulkString(w io.Writer, s []byte) (err error) {
	if _, err = w.Write([]byte(fmt.Sprintf("$%d\r\n", len(s)))); err != nil {
		return
	}
	if _, err = w.Write(s); err != nil {
		return
	}
	if _, err = w.Write([]byte("\r\n")); err != nil {
		return
	}
	return
}
func (r *Transport) WriteMessage(msg transport.Message) error {

	//buf := bytes.NewBuffer(nil)
	r.writer.WriteString("*2\r\n")
	writeBulkString(r.writer, []byte(msg.Type))
	writeBulkString(r.writer, msg.Body)
	//_, err := buf.WriteTo(r.writer)
	return r.writer.Flush()

}

type Command struct {
	Command []byte
	Key     []byte
	Args    [][]byte
}

//
// Read one request from the redis protocol
//
// This is based on the client code of Go-redis: https://github.com/alphazero/Go-Redis
func read(reader *bufio.Reader) (cmd *Command, err error) {
	buf, err := readToCRLF(reader)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case '*':
		if ll, err := strconv.Atoi(string(buf[1:])); err == nil {

			res, err := readMultiBulkData(reader, ll)
			if err != nil {
				return nil, err
			}

			if len(res) > 1 {
				return &Command{Command: res[0], Key: res[1], Args: res[2:]}, nil
			} else {
				return &Command{Command: res[0], Key: nil, Args: nil}, nil
			}
		}
	case '$':
		if ll, err := strconv.Atoi(string(buf[1:])); err == nil {
			data, err := readBulkData(reader, ll)
			if err == nil {
				return &Command{Command: data}, nil
			} else {
				return nil, err
			}
		}
	default:
		return &Command{Command: buf, Args: nil}, nil
	}
	return nil, fmt.Errorf("Could not read line. buf is '%s'", buf)
}

// hard coded error to be detected upstream
var ReadError = errors.NewError("Error Reading from Client")

// Everything below is taken from https://github.com/alphazero/Go-Redis

// panics on error (with redis.Error)
func assertCtlByte(buf []byte, b byte, info string) {
	if buf[0] != b {
		panic(fmt.Errorf("control byte for %s is not '%s' as expected - got '%s'", info, string(b), string(buf[0])))
	}
}

// panics on error (with redis.Error)
func assertNotError(e error, info string) {
	if e != nil {
		panic(e)
	}
}

// ----------------------------------------------------------------------
// Go-Redis System Errors or Bugs
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------------
// protocol i/o
// ----------------------------------------------------------------------------

// reads all bytes upto CR-LF.  (Will eat those last two bytes)
// return the line []byte up to CR-LF
// error returned is NOT ("-ERR ...").  If there is a Redis error
// that is in the line buffer returned
//

const (
	cr_byte    byte = byte('\r')
	lf_byte         = byte('\n')
	space_byte      = byte(' ')
	err_byte        = byte('-')
	ok_byte         = byte('+')
	count_byte      = byte('*')
	size_byte       = byte('$')
	num_byte        = byte(':')
	true_byte       = byte('1')
)

func readToCRLF(r *bufio.Reader) ([]byte, error) {
	//var buf []byte

	buf, _, e := r.ReadLine()
	if e != nil {
		return nil, e
	}

	return buf, nil

	var b byte
	b, e = r.ReadByte()
	if e != nil {
		return nil, e
	}
	if b != lf_byte {
		e = errors.NewError("<BUG> Expecting a Linefeed byte here!")
		return nil, e
	}
	return buf[0 : len(buf)-1], nil
}

// Reads a multibulk response of given expected elements.
//
// panics on errors (with redis.Error)
func readBulkData(r *bufio.Reader, n int) (data []byte, err error) {
	if n >= 0 {
		buffsize := n + 2
		data = make([]byte, buffsize)
		if _, e := io.ReadFull(r, data); e != nil {
			return nil, fmt.Errorf("readBulkData - ReadFull", e)
		} else {
			if data[n] != cr_byte || data[n+1] != lf_byte {
				return nil, fmt.Errorf("terminal was not crlf_bytes as expected - data[n:n+1]:%s", data[n:n+1])
			}
			data = data[:n]
		}
	}
	return
}

// Reads a multibulk response of given expected elements.
// The initial *num\r\n is assumed to have been consumed.
//
// panics on errors (with redis.Error)
func readMultiBulkData(conn *bufio.Reader, num int) ([][]byte, error) {
	data := make([][]byte, num)
	for i := 0; i < num; i++ {
		buf, err := readToCRLF(conn)
		if err != nil {
			return nil, err
		}
		if buf[0] != size_byte {
			return nil, fmt.Errorf("readMultiBulkData - expected: size_byte got: %d", buf[0])
		}

		size, e := strconv.Atoi(string(buf[1:]))
		if e != nil {
			return nil, fmt.Errorf("readMultiBulkData - Atoi parse error", e)
		}
		data[i], err = readBulkData(conn, size)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
