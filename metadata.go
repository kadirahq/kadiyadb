package kadiyadb

import (
	"io/ioutil"
	"sync"
	"time"

	goerr "github.com/go-errors/errors"
	fb "github.com/kadirahq/flatbuffers/go"
	"github.com/kadirahq/go-tools/fnutils"
	"github.com/kadirahq/go-tools/fsutils"
	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/mmap"
	"github.com/kadirahq/go-tools/secure"
	"github.com/kadirahq/kadiyadb/metadata"
)

var (
	mdsize int64
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

	memmap *mmap.File
	closed *secure.Bool
	syncfn *fnutils.Group
	dosync *secure.Bool
	rdonly bool
}

// NewMetadata creates a new metadata file at path
func NewMetadata(path string, duration, retention, resolution int64, payloadSize, segmentSize, maxROEpochs, maxRWEpochs uint32) (m *Metadata, err error) {
	mfile, err := mmap.NewFile(path, 1, true)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	if mfile.Size() == 1 {
		if n, err := mfile.Write(mdtemp); err != nil {
			return nil, goerr.Wrap(err, 0)
		} else if n != len(mdtemp) {
			return nil, goerr.Wrap(fsutils.ErrWriteSz, 0)
		}
	}

	data := mfile.MMap.Data
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

	m = &Metadata{
		Metadata: meta,
		memmap:   mfile,
		closed:   secure.NewBool(false),
		dosync:   secure.NewBool(false),
	}

	m.syncfn = fnutils.NewGroup(func() {
		if err := m.memmap.Sync(); err != nil {
			logger.Error(err, "sync metadata", path)
		}
	})

	// start syncing!
	go m.syncMetadata()

	return m, nil
}

// ReadMetadata reads the file and parses metadata.
// Changes made to this metadata will not persist.
func ReadMetadata(path string) (mdata *Metadata, err error) {
	d, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
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
	if !m.closed.Get() && !m.rdonly {
		m.dosync.Set(true)
		m.syncfn.Run()
	}
}

// Close closes metadata mmap file
func (m *Metadata) Close() (err error) {
	if m.closed.Get() {
		return nil
	}

	m.closed.Set(true)

	if !m.rdonly {
		err = m.memmap.Close()
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}

func (m *Metadata) syncMetadata() {
	for _ = range time.Tick(10 * time.Millisecond) {
		if m.closed.Get() {
			break
		}

		if m.dosync.Set(false) {
			m.syncfn.Flush()
		}

		time.Sleep(10 * time.Millisecond)
	}
}
