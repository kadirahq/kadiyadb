package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/kadirahq/kadiyadb/client"
	"github.com/kadirahq/kadiyadb/server"
)

func main() {

	var addr string
	flag.StringVar(&addr, "addr", "localhost:8000", "Host and port of the server <host>:<port>")

	flag.Parse()

	c := client.New()
	err := c.Connect(addr)

	if err != nil {
		fmt.Println("client gives error", err)
		return
	}

	testReqs := []*server.ReqFetch{}

	for i := 0; i < 100; i++ {
		testReqs = append(testReqs, &server.ReqFetch{
			Database: "kadiyadb",
			From:     0,
			To:       uint64(60000000000 * 30),
			Fields:   []string{"foo", "bar"},
		})
	}

	start := time.Now()
	for i := 0; ; i++ {
		c.Fetch(testReqs)
		if i%1000 == 0 {
			elapsed := time.Since(start)
			log.Printf("rps %2f: ", 100*float64(1000)/elapsed.Seconds())

			start = time.Now()
		}
	}
}
