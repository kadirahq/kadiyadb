package transport

import (
	"sync"

	"github.com/kadirahq/go-tools/hybrid"
)

// Transport is used to wrap and send Responses
type Transport struct {
	conn      *Conn
	writeLock *sync.Mutex
	readLock  *sync.Mutex
	buf       []byte
}

// New creates a new Transport for a connection
func New(conn *Conn) (t *Transport) {
	return &Transport{
		conn:      conn,
		writeLock: new(sync.Mutex),
		readLock:  new(sync.Mutex),
		buf:       make([]byte, 13),
	}
}

// SendBatch writes data to the connection
func (t *Transport) SendBatch(batch [][]byte, id uint64, msgType uint8) {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	sz := uint32(len(batch))

	hybrid.EncodeUint64(t.buf[:8], &id)
	hybrid.EncodeUint8(t.buf[8:9], &msgType)
	hybrid.EncodeUint32(t.buf[9:13], &sz)
	t.conn.Write(t.buf[:13])

	for _, req := range batch {
		sz := uint32(len(req))
		hybrid.EncodeUint32(t.buf[:4], &sz)
		t.conn.Write(t.buf[:4])
		t.conn.Write(req)
	}

	t.conn.Flush()
}

// ReceiveBatch reads data from the connection
func (t *Transport) ReceiveBatch() ([][]byte, uint64, uint8, error) {
	t.readLock.Lock()
	defer t.readLock.Unlock()

	var resBatch [][]byte
	var id uint64
	var msgType uint8

	bytes, err := t.conn.Read(13) // Read the header
	if err != nil {
		return resBatch, id, msgType, err
	}

	var uiSize uint32

	hybrid.DecodeUint64(bytes[:8], &id)
	hybrid.DecodeUint8(bytes[8:9], &msgType)
	hybrid.DecodeUint32(bytes[9:13], &uiSize)

	size := int(uiSize)

	resBatch = make([][]byte, size)

	var uiMsgSize uint32

	for i := 0; i < size; i++ {
		bytes, err := t.conn.Read(4)
		if err != nil {
			return resBatch, id, msgType, err
		}
		hybrid.DecodeUint32(bytes, &uiMsgSize)

		resBatch[i], err = t.conn.Read(int(uiMsgSize))
		if err != nil {
			return resBatch, id, msgType, err
		}
	}

	return resBatch, id, msgType, err
}
