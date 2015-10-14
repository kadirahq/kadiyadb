package server

import (
	"fmt"
	"sync"

	"github.com/kadirahq/kadiyadb/database"
	"github.com/kadirahq/kadiyadb/transport"
)

const (
	// MsgTypeTrack identify `Track` requests
	MsgTypeTrack = 0x00

	// MsgTypeFetch identify `Fetch` requests
	MsgTypeFetch = 0x01

	// MsgTypeSync identify `Sync` requests
	MsgTypeSync = 0x02
)

// Server is a kadiradb server
type Server struct {
	trServer *transport.Server
	dbs      map[string]*database.DB
}

// Params is used when creating a new server
type Params struct {
	Path string
	Addr string
}

var errUnknownDb []byte
var errUnknownReq []byte

func init() {
	errUnknownDb = marshalRes(&Response{
		Error: "unknown db",
	})
	errUnknownReq = marshalRes(&Response{
		Error: "unknown request",
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

	return &Server{
		trServer: server,
		dbs:      database.LoadAll(p.Path),
	}, nil
}

// Start starts processing incomming requests
func (s *Server) Start() error {
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
	defer conn.Close()

	tr := transport.New(conn)

	for {
		data, id, msgType, err := tr.ReceiveBatch()
		if err != nil {
			fmt.Println(err)
			break
		}

		switch msgType {
		case MsgTypeTrack:
			resBytes := s.handleTrack(data)
			tr.SendBatch(resBytes, id, MsgTypeFetch)
		case MsgTypeFetch:
			resBytes := s.handleFetch(data)
			tr.SendBatch(resBytes, id, MsgTypeFetch)
		}
	}
}

func (s *Server) handleTrack(trackBatch [][]byte) (resBatch [][]byte) {

	resBytes := make([][]byte, len(trackBatch))
	t := ReqTrack{}

	for i, trackData := range trackBatch {
		// Re-using `t`. But Unmarshalling will append to existing `Fields` instead
		// of overwritting for some reason. Need to slice it.
		t.Fields = t.Fields[:0]
		t.Unmarshal(trackData)

		db, ok := s.dbs[t.Database]
		if !ok {
			resBytes[i] = errUnknownDb
			continue
		}

		err := db.Track(t.Time, t.Fields, t.Total, t.Count)

		if err != nil {
			fmt.Println(err)
			resBytes[i] = marshalRes(&Response{
				Error: err.Error(),
			})
			continue
		}

		resBytes[i] = marshalRes(&Response{})
	}

	// TODO: Sync database here
	return resBytes
}

func (s *Server) handleFetch(fetchBatch [][]byte) (resBatch [][]byte) {
	resBytes := make([][]byte, len(fetchBatch))

	wg := &sync.WaitGroup{}
	wg.Add(len(fetchBatch))

	for i, fetchData := range fetchBatch {
		f := &ReqFetch{}
		f.Unmarshal(fetchData)

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

// ListDatabases returns a list of names of loaded databases
func (s *Server) ListDatabases() (list []string) {
	for db := range s.dbs {
		list = append(list, db)
	}
	return
}
