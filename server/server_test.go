package server

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/kadirahq/fastcall"
	"github.com/kadirahq/kadiyadb/database"
	"github.com/kadirahq/kadiyadb/transport"
)

func TestMain(m *testing.M) {
	flag.Parse()

	dir := "/tmp/testdbs"

	if err := os.RemoveAll(dir); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := os.MkdirAll(dir+"/test", 0777); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	data := []byte(`
	{
		"duration": 10800000000000,
		"retention": 108000000000000,
		"resolution": 60000000000,
		"maxROEpochs": 2,
		"maxRWEpochs": 2
	}`)

	if err := ioutil.WriteFile(dir+"/test/params.json", data, 0777); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	p := &Params{Addr: "localhost:3000", Path: dir}
	s, err := New(p)

	if err != nil {
		fmt.Println("NewServer gives error", err)
		os.Exit(1)
	}

	go s.Start()
	fmt.Println("started")
	time.Sleep(time.Second)
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
		{&Request{}, &Response{Error: "unknown request"}},
		{&Request{Track: &ReqTrack{Database: "bar"}}, &Response{Error: "unknown db"}},
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

func TestBatch(t *testing.T) {
	conn, err := fastcall.Dial("localhost:3000")
	if err != nil {
		t.Fatal("Fastcall dial gives error", err)
	}

	tracks := []*Request{}

	for i := 0; i < 100; i++ {
		tracks = append(tracks, &Request{
			Track: &ReqTrack{
				Database: "test",
				Time:     10,
				Total:    3.14,
				Count:    1,
				Fields:   []string{"foo", "bar"},
			},
		})
	}

	testReq := RequestBatch{
		Batch: tracks,
	}

	data, err := testReq.Marshal()

	conn.Write(data)

	tr := transport.New(conn)
	b, _, err := tr.ReceiveBatch()

	tracks = []*Request{}

	tracks = append(tracks,
		&Request{
			Fetch: &ReqFetch{
				Database: "test",
				From:     0,
				To:       60000000000,
				Fields:   []string{"foo", "bar"},
			},
		},

		&Request{
			Fetch: &ReqFetch{
				Database: "test",
				From:     0,
				To:       60000000000,
				Fields:   []string{"foo", "bar"},
			},
		},
	)

	testReq = RequestBatch{
		Id:    1,
		Batch: tracks,
	}

	data, err = testReq.Marshal()
	conn.Write(data)

	b, _, err = tr.ReceiveBatch()

	for _, res := range b {
		r := Response{}
		err := r.Unmarshal(res)

		//TODO: assert correctness received data

		if err != nil {
			t.Fatal(err)
		}

	}
}

func BenchmarkServer(b *testing.B) {

	conn, err := fastcall.Dial("localhost:3000")
	if err != nil {
		b.Fatal("Fastcall dial gives error", err)
	}

	testReq := RequestBatch{}

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

// Many Track requests in one batch
func BenchmarkReqTrack(b *testing.B) {
	conn, err := fastcall.Dial("localhost:3000")
	if err != nil {
		b.Fatal("Fastcall dial gives error", err)
	}

	tracks := []*Request{}

	for i := 0; i < b.N; i++ {
		tracks = append(tracks, &Request{
			Track: &ReqTrack{
				Database: "test",
				Time:     uint64(time.Now().UnixNano()),
				Total:    3.14,
				Count:    1,
				Fields:   []string{"foo", "bar"},
			},
		})
	}

	testReq := RequestBatch{
		Batch: tracks,
	}

	data, err := testReq.Marshal()
	if err != nil {
		b.Fatal("Marshal gives error", err)
	}

	b.ResetTimer()

	conn.Write(data)
	conn.Read()
}
