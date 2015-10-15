package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/kadirahq/go-tools/function"
	"github.com/kadirahq/kadiyadb/database"
	"github.com/kadirahq/kadiyadb/transport"
)

const (
	// MsgTypeTrack identify `Track` requests
	MsgTypeTrack = 0x00

	// MsgTypeFetch identify `Fetch` requests
	MsgTypeFetch = 0x01

	// syncPeriod is the time between database syncs in milliseconds
	syncPeriod = 100
)

// Server is a kadiradb server
type Server struct {
	trServer *transport.Server
	dbs      map[string]*database.DB
	sync     *function.Group
}

// Params is used when creating a new server
type Params struct {
	Path string
	Addr string
}

var errUnknownDb []byte
var errUnknownReq []byte
var errNotParsable []byte

func init() {
	errUnknownDb = marshalRes(&Response{
		Error: "unknown db",
	})
	errUnknownReq = marshalRes(&Response{
		Error: "unknown request",
	})
	errNotParsable = marshalRes(&Response{
		Error: "can't parse",
	})
}

// New create a transport connection that clients can send to.
// It statrs listning but does not actually start handling incomming requests.
// But none of the incomming requests are lost. To process incomming requests
// call Start.
func New(p *Params) (*Server, error) {
	server, err := transport.Serve(p.Addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		trServer: server,
		dbs:      database.LoadAll(p.Path),
	}

	s.sync = function.NewGroup(s.Sync)
	return s, nil
}

// Start starts processing incomming requests
func (s *Server) Start() error {
	c := time.Tick(syncPeriod * time.Millisecond)

	go func() {
		for _ = range c {
			s.sync.Flush()
		}
	}()

	for {
		conn, err := s.trServer.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn *transport.Conn) {
	tr := transport.New(conn)

	for {
		data, id, msgType, err := tr.ReceiveBatch()
		if err != nil {
			fmt.Println(err)
			break
		}

		go s.handleMessage(tr, data, id, msgType)
	}

	err := conn.Close()
	if err != nil {
		fmt.Println("Error while closing connection", err)
	}
}

func (s *Server) handleMessage(tr *transport.Transport, data [][]byte, id uint64, msgType uint8) {
	var err error

	switch msgType {
	case MsgTypeTrack:
		resData := s.handleTrack(data)
		err = tr.SendBatch(resData, id, MsgTypeTrack)
	case MsgTypeFetch:
		resData := s.handleFetch(data)
		err = tr.SendBatch(resData, id, MsgTypeFetch)
	}
	if err != nil {
		fmt.Printf("Error while sending batch (id: %d) %s", id, err)
	}
}

func (s *Server) handleTrack(trackBatch [][]byte) (resBatch [][]byte) {

	resBytes := make([][]byte, len(trackBatch))
	t := ReqTrack{}

	for i, trackData := range trackBatch {
		// Re-using `t`. But Unmarshalling will append to existing `Fields` instead
		// of overwritting for some reason. Need to slice it.
		t.Fields = t.Fields[:0]
		err := t.Unmarshal(trackData)
		if err != nil {
			resBytes[i] = errNotParsable
			continue
		}

		db, ok := s.dbs[t.Database]
		if !ok {
			resBytes[i] = errUnknownDb
			continue
		}

		err = db.Track(t.Time, t.Fields, t.Total, t.Count)

		if err != nil {
			fmt.Println(err)
			resBytes[i] = marshalRes(&Response{
				Error: err.Error(),
			})
			continue
		}

		resBytes[i] = marshalRes(&Response{})
	}

	s.sync.Run()
	return resBytes
}

func (s *Server) handleFetch(fetchBatch [][]byte) (resBatch [][]byte) {
	resBytes := make([][]byte, len(fetchBatch))

	wg := &sync.WaitGroup{}
	wg.Add(len(fetchBatch))

	for i, fetchData := range fetchBatch {
		f := &ReqFetch{}
		err := f.Unmarshal(fetchData)
		if err != nil {
			resBytes[i] = errNotParsable
			continue
		}

		go func(f *ReqFetch, i int) {
			db, ok := s.dbs[f.Database]
			if !ok {
				resBytes[i] = errUnknownDb
				wg.Done()
			}

			handler := func(result []*database.Chunk, err error) {
				if err != nil {
					fmt.Println(err)
					resBytes[i] = marshalRes(&Response{
						Error: err.Error(),
					})
					wg.Done()
					return
				}

				resBytes[i] = marshalRes(&Response{Fetch: &ResFetch{Chunks: result}})
				wg.Done()
			}

			db.Fetch(f.From, f.To, f.Fields, handler)
		}(f, i)
	}
	wg.Wait()

	return resBytes
}

func marshalRes(res *Response) (resBytes []byte) {
	resBytes, _ = res.Marshal()
	return
}

// Sync syncs every database in the server
func (s *Server) Sync() {
	for dbname, db := range s.dbs {
		err := db.Sync()
		if err != nil {
			fmt.Printf("Error while syncing database (name: %s) %s", dbname, err)
		}
	}
}

// ListDatabases returns a list of names of loaded databases
func (s *Server) ListDatabases() (list []string) {
	for db := range s.dbs {
		list = append(list, db)
	}
	return
}
