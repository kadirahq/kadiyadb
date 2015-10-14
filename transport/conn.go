package transport

import (
	"bufio"
	"io"
	"net"
)

const defaultBufferSize = 8192

// Conn is a Transport connection
type Conn struct {
	writer *bufio.Writer
	reader *bufio.Reader
	closer io.Closer
}

// NewConn creates a new Transport connection
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		writer: bufio.NewWriterSize(conn, defaultBufferSize),
		reader: bufio.NewReaderSize(conn, defaultBufferSize),
		closer: conn,
	}
}

// Dial creates a connection to given address
func Dial(addr string) (c *Conn, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewConn(conn), nil
}

// Write writes to the connection
func (conn *Conn) Write(buffer []byte) error {
	toWrite := buffer[:]
	for len(toWrite) > 0 {
		n, err := conn.writer.Write(toWrite)
		if (err != nil) && (err != io.ErrShortWrite) {
			return err
		}

		toWrite = toWrite[n:]
	}

	return nil
}

// Read reads `n` number of bytes from the connection
func (conn *Conn) Read(n int) ([]byte, error) {
	buffer := make([]byte, n)

	toRead := buffer[:]
	for len(toRead) > 0 {
		read, err := conn.reader.Read(toRead)
		if err != nil {
			return nil, err
		}

		toRead = toRead[read:]
	}

	return buffer, nil
}

// Flush flushes the buffer
func (conn *Conn) Flush() error {
	err := conn.writer.Flush()
	for err == io.ErrShortWrite {
		err = conn.writer.Flush()
	}
	return err
}

// Close closes the connection
func (conn *Conn) Close() (err error) {
	conn.Flush()

	if err := conn.closer.Close(); err != nil {
		return err
	}

	return nil
}
