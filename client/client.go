package client

import (
	"fmt"
	"sync/atomic"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/kadiyadb/server"
	"github.com/kadirahq/kadiyadb/transport"
)

// Client is a kadiyadb Client
type Client struct {
	conn     *fastcall.Conn
	tran     *transport.Transport
	inflight map[int64]chan [][]byte
	nextID   int64
}

// New creates a new kadiyadb Client
func New() *Client {
	return &Client{
		inflight: make(map[int64]chan [][]byte, 1),
	}
}

// Connect connects the Client to a kadiyadb server
func (c *Client) Connect(addr string) error {
	conn, err := fastcall.DialBuf(addr)

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
		data, id, _ := c.tran.ReceiveBatch()
		ch, ok := c.inflight[id]

		if !ok {
			fmt.Println("Unknown response id")
			continue
		}

		ch <- data
	}
}

func (c *Client) call(b [][]byte) [][]byte {
	ch := make(chan [][]byte, 1)
	id := c.getNextID()
	c.inflight[id] = ch

	c.tran.SendBatch(b, id)

	return <-ch
}

func (c *Client) retrieve(requests []*server.Request) ([]*server.Response, error) {
	data := make([][]byte, len(requests))

	for i, req := range requests {
		data[i], _ = req.Marshal()
	}

	resData := c.call(data)

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

func (c *Client) getNextID() (id int64) {
	return atomic.AddInt64(&c.nextID, 1)
}

// Track tracks kadiyadb points
func (c *Client) Track(tracks []*server.ReqTrack) ([]*server.Response, error) {
	requests := make([]*server.Request, len(tracks))

	for i, track := range tracks {
		requests[i] = &server.Request{
			Track: track,
		}
	}

	return c.retrieve(requests)
}

// Fetch fetches kadiyadb point data
func (c *Client) Fetch(fetches []*server.ReqFetch) ([]*server.Response, error) {
	requests := make([]*server.Request, len(fetches))

	for i, fetch := range fetches {
		requests[i] = &server.Request{
			Fetch: fetch,
		}
	}

	return c.retrieve(requests)
}
