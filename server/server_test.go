package server

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

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

	go func() {
		err := s.Start()
		if err != nil {
			fmt.Println(err)
		}
	}()

	time.Sleep(time.Second) // give a second to the server to start
	os.Exit(m.Run())
}

func TestBatch(t *testing.T) {
	conn, err := transport.Dial("localhost:3000")
	if err != nil {
		t.Fatal("Dial gives error", err)
	}
	tr := transport.New(conn)

	tracks := []*ReqTrack{}

	for i := 0; i < 100; i++ {
		tracks = append(tracks, &ReqTrack{
			Database: "test",
			Time:     10,
			Total:    3.14,
			Count:    1,
			Fields:   []string{"foo", "bar"},
		})
	}

	data := make([][]byte, len(tracks))

	// Create a batch with 100 tracks
	for i, req := range tracks {
		data[i], err = req.Marshal()
		if err != nil {
			t.Fatal(err)
		}
	}

	err = tr.SendBatch(data, 1, MsgTypeTrack)
	if err != nil {
		t.Fatal(err)
	}

	b, _, _, err := tr.ReceiveBatch()
	if err != nil {
		t.Fatal(err)
	}

	fetches := []*ReqFetch{}

	fetches = append(fetches,
		&ReqFetch{
			Database: "test",
			From:     0,
			To:       60000000000,
			Fields:   []string{"foo", "bar"},
		},

		&ReqFetch{
			Database: "test",
			From:     0,
			To:       60000000000,
			Fields:   []string{"foo", "bar"},
		},
	)

	data = make([][]byte, len(fetches))

	// Create a batch with 2 fetches
	for i, req := range fetches {
		data[i], err = req.Marshal()
		if err != nil {
			t.Fatal("Marshal gives error", err)
		}
	}

	err = tr.SendBatch(data, 2, MsgTypeFetch)
	if err != nil {
		t.Fatal(err)
	}

	b, _, _, err = tr.ReceiveBatch()
	if err != nil {
		t.Fatal(err)
	}

	for _, res := range b {
		r := Response{}
		err := r.Unmarshal(res)

		//TODO: assert correctness received data

		if err != nil {
			t.Fatal(err)
		}

	}
}

// Many Track requests in one batch
func BenchmarkReqTrack(b *testing.B) {
	conn, err := transport.Dial("localhost:3000")
	tr := transport.New(conn)
	if err != nil {
		b.Fatal("Dial gives error", err)
	}

	tracks := []*ReqTrack{}

	for i := 0; i < b.N; i++ {
		tracks = append(tracks, &ReqTrack{
			Database: "test",
			Time:     uint64(time.Now().UnixNano()),
			Total:    3.14,
			Count:    1,
			Fields:   []string{"foo", "bar"},
		})
	}

	data := make([][]byte, len(tracks))
	b.ResetTimer()

	// Create a batch
	for i, req := range tracks {
		data[i], _ = req.Marshal()
	}

	err = tr.SendBatch(data, 2, MsgTypeTrack)
	if err != nil {
		b.Fatal("Dial gives error", err)
	}

	_, _, _, err = tr.ReceiveBatch()
	if err != nil {
		b.Fatal("Dial gives error", err)
	}
}
