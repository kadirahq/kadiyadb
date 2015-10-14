package transport

import "net"

// Server listens for new connections
type Server struct {
	lsnr net.Listener
}

// Serve creates a listener and accepts connections
func Serve(addr string) (s *Server, err error) {
	lsnr, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{lsnr: lsnr}, nil
}

// Close stops accepting connections
func (s *Server) Close() (err error) {
	if err := s.lsnr.Close(); err != nil {
		return err
	}

	return nil
}

// Accept returns a channel of connections
func (s *Server) Accept() (c *Conn, err error) {
	conn, err := s.lsnr.Accept()
	if err != nil {
		return nil, err
	}

	return NewConn(conn), nil
}
