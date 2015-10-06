package server

import (
	"fmt"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/kadiyadb/database"
)

// Server is a kadiradb server
type Server struct {
	fcServer *fastcall.Server
	dbs      map[string]*database.DB
}

// Params is used when creating a new server
type Params struct {
	Path     string
	Hostport string
}

// New create a fastcall connection that clients can send to.
// It statrs listning but does not actually start handling incomming requests.
// But none of the incomming requests are lost. To process incomming requests
// call Start.
func New(p *Params) (*Server, error) {
	server, err := fastcall.Serve(p.Hostport)
	if err != nil {
		return nil, err
	}

	return &Server{
		fcServer: server,
		dbs:      database.LoadDatabases(p.Path),
	}, nil
}

// Start starts processing incomming requests
func (s *Server) Start() error {
	for {
		conn, err := s.fcServer.Accept()
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

	for {
		data, err = conn.Read()
		if err != nil {
			fmt.Println(err)
			break
		}

		req := Request{}
		err = req.Unmarshal(data)
		if err != nil {
			fmt.Println(err)
			continue
		}

		go func() {
			b := s.handleRequest(&req)
			conn.Write(b)
		}()
	}
}

func (s *Server) handleRequest(req *Request) (res []byte) {
	db, ok := s.dbs[req.Database]
	if !ok {
		return marshalRes(&Response{
			Error: "unknown db",
		})
	}

	if t := req.GetTrack(); t != nil {

		err := db.Track(t.Time, t.Fields, t.Total, t.Count)
		if err != nil {
			return marshalRes(&Response{
				Error: err.Error(),
			})
		}

		return marshalRes(&Response{
			Track: &ResTrack{},
		})

	} else if f := req.GetFetch(); f != nil {

		resBytes := []byte{}

		handler := func(result []*database.Chunk, err error) {
			resBytes = marshalRes(&Response{Fetch: &ResFetch{Chunks: result}})
		}
		db.Fetch(f.From, f.To, f.Fields, handler)

		return resBytes

	} else if s := req.GetSync(); s != nil {

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

	return marshalRes(&Response{})
}

func marshalRes(res *Response) (resBytes []byte) {
	resBytes, _ = res.Marshal()
	return
}
