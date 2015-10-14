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

	testReqs := []*server.ReqTrack{}

	for i := 0; i < 100; i++ {
		testReqs = append(testReqs, &server.ReqTrack{
			Database: "kadiyadb",
			Time:     uint64(i * 60000000000),
			Fields:   []string{"foo", "bar"},
			Total:    3.14,
			Count:    1,
		})
	}

	start := time.Now()
	for i := 0; ; i++ {
		c.Track(testReqs)
		if i%1000 == 0 {
			elapsed := time.Since(start)
			log.Printf("rps %2f: ", 100*float64(1000)/elapsed.Seconds())

			start = time.Now()
		}
	}

}
