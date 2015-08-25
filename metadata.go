package kadiyadb

import (
	"io/ioutil"
	"sync"
	"time"

	fb "github.com/kadirahq/flatbuffers/go"
	"github.com/kadirahq/go-tools/dmutex"
	"github.com/kadirahq/go-tools/mmap"
	"github.com/kadirahq/go-tools/secure"
	"github.com/kadirahq/kadiyadb/metadata"
)

var (
	// mdsize is the size of the metadata file
	mdsize int64

	// mdtemp is an empty metadata buffer
	mdtemp []byte
)

func init() {
	// Create an empty metadata buffer which can be used as a template later.
	// When creating the table, always use non-zero values otherwise it will not
	// allocate space to store these fields. Set them to zero values later.

	b := fb.NewBuilder(0)
	metadata.MetadataStart(b)
	metadata.MetadataAddDuration(b, -1)
	metadata.MetadataAddRetention(b, -1)
	metadata.MetadataAddResolution(b, -1)
	metadata.MetadataAddPayloadSize(b, 1)
	metadata.MetadataAddSegmentSize(b, 1)
	metadata.MetadataAddMaxROEpochs(b, 1)
	metadata.MetadataAddMaxRWEpochs(b, 1)
	b.Finish(metadata.MetadataEnd(b))

	mdtemp = b.Bytes[b.Head():]
	mdsize = int64(len(mdtemp))

	meta := metadata.GetRootAsMetadata(mdtemp, 0)
	meta.SetDuration(0)
	meta.SetRetention(0)
	meta.SetResolution(0)
	meta.SetPayloadSize(0)
	meta.SetSegmentSize(0)
	meta.SetMaxROEpochs(0)
	meta.SetMaxRWEpochs(0)
}

// Metadata persists segfile information to disk in flatbuffer format
type Metadata struct {
	sync.RWMutex
	*metadata.Metadata

	mmap   mmap.File
	closed *secure.Bool
	dmutex *dmutex.Mutex
	rdonly bool
}

// NewMetadata creates a new metadata file at fpath
func NewMetadata(fpath string, duration, retention, resolution int64, payloadSize, segmentSize, maxROEpochs, maxRWEpochs uint32) (mdata *Metadata, err error) {
	m, err := mmap.New(&mmap.Options{Path: fpath})
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	if m.Size() == 0 {
		n, err := m.WriteAt(mdtemp, 0)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		} else if int64(n) != mdsize {
			Logger.Trace(ErrWrite)
			return nil, ErrWrite
		}
	}

	data := m.Data()
	meta := metadata.GetRootAsMetadata(data, 0)
	if meta.Duration() == 0 {
		meta.SetDuration(duration)
	}

	if meta.Retention() == 0 {
		meta.SetRetention(retention)
	}

	if meta.Resolution() == 0 {
		meta.SetResolution(resolution)
	}

	if meta.PayloadSize() == 0 {
		meta.SetPayloadSize(payloadSize)
	}

	if meta.SegmentSize() == 0 {
		meta.SetSegmentSize(segmentSize)
	}

	if meta.MaxROEpochs() == 0 {
		meta.SetMaxROEpochs(maxROEpochs)
	}

	if meta.MaxRWEpochs() == 0 {
		meta.SetMaxRWEpochs(maxRWEpochs)
	}

	mdata = &Metadata{
		Metadata: meta,
		mmap:     m,
		closed:   secure.NewBool(false),
		dmutex:   dmutex.New(),
	}

	// batch sync requests
	go mdata.startSync()

	return mdata, nil
}

// ReadMetadata reads the file and parses metadata.
// Changes made to this metadata will not persist.
func ReadMetadata(fpath string) (mdata *Metadata, err error) {
	d, err := ioutil.ReadFile(fpath)
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	meta := metadata.GetRootAsMetadata(d, 0)
	mdata = &Metadata{
		Metadata: meta,
		closed:   secure.NewBool(false),
		rdonly:   true,
	}

	return mdata, nil
}

// Sync syncs the memory map to the disk
func (m *Metadata) Sync() {
	if !m.rdonly {
		m.dmutex.Wait()
	}
}

// Close closes metadata mmap file
func (m *Metadata) Close() (err error) {
	if m.closed.Get() {
		Logger.Error(ErrClosed)
		return nil
	}
	m.closed.Set(true)

	if m.mmap != nil {
		err = m.mmap.Close()
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	return nil
}

func (m *Metadata) startSync() {
	for !m.closed.Get() {
		// do an mmap msync only if it's requested
		m.dmutex.Flush(func() {
			if err := m.mmap.Sync(); err != nil {
				Logger.Error(err)
			}
		})

		// wait 10ms before next flush
		time.Sleep(10 * time.Millisecond)
	}
}
