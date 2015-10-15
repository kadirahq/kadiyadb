package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
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
	var count, made int64

	for i := 0; i < 100; i++ {
		testReqs = append(testReqs, &server.ReqTrack{
			Database: "kadiyadb",
			Time:     uint64(i * 60000000000),
			Fields:   []string{"foo", "bar"},
			Total:    3.14,
			Count:    1,
		})
	}

	do := func() {
		for {
			atomic.AddInt64(&made, 1)
			c.Track(testReqs)
			atomic.AddInt64(&count, 1)
		}
	}

	parallel := 1000

	for i := 0; i < parallel; i++ {
		go do()
	}

	for {
		time.Sleep(time.Second)
		log.Println(100*count, "/", 100*made)
		atomic.StoreInt64(&count, 0)
		atomic.StoreInt64(&made, 0)
	}

}
