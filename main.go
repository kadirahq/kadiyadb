package main

import (
	"flag"
	"fmt"

	"github.com/kadirahq/kadiyadb/server"
)

func main() {
	p := &server.Params{}
	flag.StringVar(&p.Path, "path", "/data/", "Where the databases are located")
	flag.StringVar(&p.Addr, "addr", "localhost:8000", "Host and port of the server <host>:<port>")

	flag.Parse()

	s, err := server.New(p)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Listening on %s", p.Addr)
	s.Start()
}
