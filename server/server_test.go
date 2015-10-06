package server

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/kadiyadb/database"
)

func TestMain(m *testing.M) {
	flag.Parse()
	p := &Params{Hostport: "localhost:3000"}
	s, err := New(p)

	if err != nil {
		fmt.Println("NewServer gives error", err)
		return
	}

	go s.Start()
	os.Exit(m.Run())
}

func TestHandleRequest(t *testing.T) {
	ts := Server{}

	ts.dbs = map[string]*database.DB{
		"foo": &database.DB{},
	}

	tests := []struct {
		req *Request
		res *Response
	}{
		{&Request{}, &Response{Error: "unknown db"}},
		{&Request{Database: "bar"}, &Response{Error: "unknown db"}},
		{&Request{Database: "foo"}, &Response{Error: ""}},
	}

	for _, test := range tests {
		resBytes := ts.handleRequest(test.req)
		res := &Response{}
		res.Unmarshal(resBytes)

		if test.res.Error != res.Error {
			t.Fatalf("Wrong error. Expected: %s Got: %s", test.res.Error, res.Error)
		}
	}
}

func BenchmarkServer(b *testing.B) {

	conn, err := fastcall.Dial("localhost:3000")
	if err != nil {
		b.Fatal("Fastcall dial gives error", err)
	}

	testReq := Request{}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, err := testReq.Marshal()
			if err != nil {
				b.Fatal("Marshal gives error", err)
			}
			conn.Write(data)
			conn.Read()
		}
	})

}
