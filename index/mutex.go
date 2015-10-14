package index

import "sync"

// pbmutex is sync.RWMutex with pb methods
// TODO find better way to add sync.RWMutex
type pbmutex struct {
	sync.RWMutex
}

func (p *pbmutex) Equal(that interface{}) bool        { return false }
func (p *pbmutex) Size() int                          { return 8 }
func (p *pbmutex) MarshalTo(data []byte) (int, error) { return 8, nil }
func (p *pbmutex) Unmarshal(data []byte) error        { return nil }
