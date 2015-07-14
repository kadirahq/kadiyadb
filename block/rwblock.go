package block

import (
	"os"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
)

type rwblock struct {
	*block
	segments map[int64]*os.File
}

func newRWBlock(b *block, options *Options) (blk *rwblock, err error) {
	// TODO: implement

	blk = &rwblock{
		block: b,
	}

	return blk, nil
}

func (b *rwblock) Add() (id int64, err error) {
	// TODO: implement
	return 0, nil
}

func (b *rwblock) Put(id, pos int64, pld []byte) (err error) {
	// TODO: implement
	return nil
}

func (b *rwblock) Get(id, start, end int64) (res [][]byte, err error) {
	// TODO: implement
	return nil, nil
}

func (b *rwblock) Close() (err error) {
	err = b.block.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}
