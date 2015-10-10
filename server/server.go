package server

import (
	"fmt"
	"sync"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/kadiyadb/database"
	"github.com/kadirahq/kadiyadb/transport"
)

// Server is a kadiradb server
type Server struct {
	fcServer *fastcall.Server
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

// New create a fastcall connection that clients can send to.
// It statrs listning but does not actually start handling incomming requests.
// But none of the incomming requests are lost. To process incomming requests
// call Start.
func New(p *Params) (*Server, error) {
	server, err := fastcall.Serve(p.Addr)
	if err != nil {
		return nil, err
	}

	return &Server{
		fcServer: server,
		dbs:      database.LoadAll(p.Path),
	}, nil
}

// Start starts processing incomming requests
func (s *Server) Start() error {
	for {
		conn, err := s.fcServer.AcceptBuf()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn *fastcall.Conn) {
	defer conn.Close()

	var data []byte
	var err error

	t := transport.New(conn)

	for {
		data, err = conn.Read()

		if err != nil {
			fmt.Println(err)
			break
		}

		batch := RequestBatch{}
		err = batch.Unmarshal(data)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if err != nil {
			fmt.Println(err)
			continue
		}

		resBytes := make([][]byte, len(batch.Batch))

		var wg sync.WaitGroup

		for i, req := range batch.Batch {
			if f := req.GetTrack(); f != nil {
				b := s.handleRequest(req)
				resBytes[i] = b
				continue
			}

			wg.Add(1)

			go func(req *Request, i int) {
				b := s.handleRequest(req)
				resBytes[i] = b
				wg.Done()
			}(req, i)
		}

		wg.Wait()

		t.SendBatch(resBytes, batch.Id)
		conn.FlushWriter()
	}
}

func (s *Server) handleRequest(req *Request) (res []byte) {

	if t := req.GetTrack(); t != nil {
		db, ok := s.dbs[t.Database]
		if !ok {
			return errUnknownDb
		}

		err := db.Track(t.Time, t.Fields, t.Total, t.Count)
		if err != nil {
			fmt.Println(err)
			return marshalRes(&Response{
				Error: err.Error(),
			})
		}

		return marshalRes(&Response{
			Track: &ResTrack{},
		})

	} else if f := req.GetFetch(); f != nil {
		db, ok := s.dbs[f.Database]
		if !ok {
			return errUnknownDb
		}

		resBytes := []byte{}

		handler := func(result []*database.Chunk, err error) {
			resBytes = marshalRes(&Response{Fetch: &ResFetch{Chunks: result}})
		}
		db.Fetch(f.From, f.To, f.Fields, handler)

		return resBytes

	} else if sync := req.GetSync(); sync != nil {
		db, ok := s.dbs[sync.Database]
		if !ok {
			return errUnknownDb
		}

		err := db.Sync()
		if err != nil {
			return marshalRes(&Response{
				Error: err.Error(),
			})
		}

		return marshalRes(&Response{
			Error: err.Error(),
		})
	}

	return errUnknownReq
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
