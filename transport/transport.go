package transport

import (
	"sync"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/go-tools/hybrid"
)

// Transport is used to wrap and send Responses
type Transport struct {
	conn *fastcall.Conn
	mtx  *sync.Mutex
	d    []byte
}

// New creates a new Transport for a connection
func New(conn *fastcall.Conn) (t *Transport) {
	return &Transport{
		conn: conn,
		mtx:  new(sync.Mutex),
		d:    make([]byte, 8),
	}
}

// SendBatch writes data to the connection
func (t *Transport) SendBatch(batch [][]byte, id int64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	hybrid.EncodeInt64(t.d[:8], &id)
	t.conn.Write(t.d[:8])

	a := uint32(len(batch))
	hybrid.EncodeUint32(t.d[:4], &a)
	t.conn.Write(t.d[:4])

	for _, req := range batch {
		t.conn.Write(req)
	}
}

// ReceiveBatch reads data from the connection
func (t *Transport) ReceiveBatch() (resBatch [][]byte, id int64, err error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	bytes, err := t.conn.Read()
	if err != nil {
		return
	}
	hybrid.DecodeInt64(bytes[:8], &id)

	bytes, err = t.conn.Read()
	if err != nil {
		return
	}
	var uisize uint32
	hybrid.DecodeUint32(bytes[:4], &uisize)

	size := int(uisize)

	resBatch = make([][]byte, size)

	for i := 0; i < size; i++ {
		resBatch[i], err = t.conn.Read()
		if err != nil {
			return
		}
	}

	return
}
