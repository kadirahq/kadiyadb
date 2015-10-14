package client

import (
	"fmt"
	"sync/atomic"

	"github.com/kadirahq/kadiyadb/server"
	"github.com/kadirahq/kadiyadb/transport"
)

// Client is a kadiyadb Client
type Client struct {
	conn     *transport.Conn
	tran     *transport.Transport
	inflight map[uint64]chan [][]byte
	nextID   uint64
}

// New creates a new kadiyadb Client
func New() *Client {
	return &Client{
		inflight: make(map[uint64]chan [][]byte, 1),
	}
}

// Connect connects the Client to a kadiyadb server
func (c *Client) Connect(addr string) error {
	conn, err := transport.Dial(addr)

	if err != nil {
		return err
	}

	c.conn = conn
	c.tran = transport.New(conn)
	go c.readConn()
	return nil
}

func (c *Client) readConn() {
	for {
		data, id, _, err := c.tran.ReceiveBatch() // `msgType` is dropped its not
		//important for the client

		if err != nil {
			fmt.Println(err)
		}

		ch, ok := c.inflight[id]

		if !ok {
			fmt.Println("Unknown response id")
			continue
		}

		ch <- data
	}
}

func (c *Client) call(b [][]byte, msgType uint8) [][]byte {
	ch := make(chan [][]byte, 1)
	id := c.getNextID()
	c.inflight[id] = ch

	c.tran.SendBatch(b, id, msgType)

	return <-ch
}

func (c *Client) retrieve(data [][]byte, msgType uint8) ([]*server.Response, error) {

	resData := c.call(data, msgType)

	responses := make([]*server.Response, len(resData))

	for i, data := range resData {
		responses[i] = new(server.Response)
		err := responses[i].Unmarshal(data)

		if err != nil {
			return nil, err
		}
	}

	return responses, nil
}

func (c *Client) getNextID() (id uint64) {
	return atomic.AddUint64(&c.nextID, 1)
}

// Track tracks kadiyadb points
func (c *Client) Track(tracks []*server.ReqTrack) ([]*server.Response, error) {
	data := make([][]byte, len(tracks))

	for i, track := range tracks {
		data[i], _ = track.Marshal()
	}

	return c.retrieve(data, server.MsgTypeTrack)
}

// Fetch fetches kadiyadb point data
func (c *Client) Fetch(fetches []*server.ReqFetch) ([]*server.Response, error) {
	data := make([][]byte, len(fetches))

	for i, fetch := range fetches {
		data[i], _ = fetch.Marshal()
	}

	return c.retrieve(data, server.MsgTypeFetch)
}
