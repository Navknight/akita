package writearound

import (
	"fmt"

	"github.com/sarchlab/akita/v3/mem/cache"
	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/sim"
	"github.com/sarchlab/akita/v3/tracing"
)

type bottomParser struct {
	cache *Cache
}

func (p *bottomParser) Tick(now sim.VTimeInSec) bool {
	item := p.cache.bottomPort.Peek()
	if item == nil {
		return false
	}

	switch rsp := item.(type) {
	case *mem.WriteDoneRsp:
		return p.processDoneRsp(now, rsp)
	case *mem.DataReadyRsp:
		return p.processDataReady(now, rsp)
	default:
		panic("cannot process response")
	}
}

func (p *bottomParser) processDoneRsp(
	now sim.VTimeInSec,
	done *mem.WriteDoneRsp,
) bool {
	trans := p.findTransactionByWriteToBottomID(done.GetRspTo())
	if trans == nil || trans.fetchAndWrite {
		p.cache.bottomPort.Retrieve(now)
		return true
	}

	for _, t := range trans.preCoalesceTransactions {
		t.done = true
	}

	p.removeTransaction(trans)
	p.cache.bottomPort.Retrieve(now)

	tracing.TraceReqFinalize(trans.writeToBottom, p.cache)
	tracing.EndTask(trans.id, p.cache)

	return true
}

func (p *bottomParser) processDataReady(
	now sim.VTimeInSec,
	dr *mem.DataReadyRsp,
) bool {
	trans := p.findTransactionByReadToBottomID(dr.GetRspTo())
	if trans == nil {
		p.cache.bottomPort.Retrieve(now)
		return true
	}
	pid := trans.readToBottom.PID
	bankBuf := p.getBankBuf(trans.block)
	if !bankBuf.CanPush() {
		return false
	}

	addr := trans.Address()
	cachelineID := (addr >> p.cache.log2BlockSize) << p.cache.log2BlockSize
	data := dr.Data
	dirtyMask := make([]bool, 1<<p.cache.log2BlockSize)
	mshrEntry := p.cache.mshr.Query(pid, cachelineID)
	p.mergeMSHRData(mshrEntry, data, dirtyMask)
	p.finalizeMSHRTrans(mshrEntry, data, now)
	p.cache.mshr.Remove(pid, cachelineID)

	trans.bankAction = bankActionWriteFetched
	trans.data = data
	trans.writeFetchedDirtyMask = dirtyMask
	bankBuf.Push(trans)

	p.removeTransaction(trans)
	p.cache.bottomPort.Retrieve(now)

	tracing.TraceReqFinalize(trans.readToBottom, p.cache)

	return true
}

func (p *bottomParser) mergeMSHRData(
	mshrEntry *cache.MSHREntry,
	data []byte,
	dirtyMask []bool,
) {
	for _, t := range mshrEntry.Requests {
		trans := t.(*transaction)

		if trans.write == nil {
			continue
		}

		write := trans.write
		offset := write.Address - mshrEntry.Block.Tag
		for i := 0; i < len(write.Data); i++ {
			if write.DirtyMask[i] {
				data[offset+uint64(i)] = write.Data[i]
				dirtyMask[offset+uint64(i)] = true
			}
		}
	}
}

func (p *bottomParser) finalizeMSHRTrans(
	mshrEntry *cache.MSHREntry,
	data []byte,
	now sim.VTimeInSec,
) {
	// Separate demand and prefetch requests for better control
	demandRequests := []*transaction{}
	prefetchRequests := []*transaction{}

	for _, t := range mshrEntry.Requests {
		trans := t.(*transaction)
		if trans.isPrefetch {
			prefetchRequests = append(prefetchRequests, trans)
		} else {
			demandRequests = append(demandRequests, trans)
		}
	}

	// Process demand requests first
	for _, trans := range demandRequests {
		if trans.read != nil {
			// Verify the data length meets the block size requirement
			expectedSize := uint64(1 << p.cache.log2BlockSize)
			if uint64(len(data)) != expectedSize {
				panic(fmt.Sprintf("Data length mismatch: expected %d bytes, got %d bytes",
					expectedSize, len(data)))
			}

			for _, preCTrans := range trans.preCoalesceTransactions {
				read := preCTrans.read
				offset := read.Address - mshrEntry.Block.Tag

				// Range check to catch offset calculation errors
				if offset >= uint64(len(data)) || offset+read.AccessByteSize > uint64(len(data)) {
					panic(fmt.Sprintf("Out of bounds access: offset=%d, size=%d, data_len=%d",
						offset, read.AccessByteSize, len(data)))
				}

				preCTrans.data = data[offset : offset+read.AccessByteSize]
				preCTrans.done = true
			}
		} else {
			for _, preCTrans := range trans.preCoalesceTransactions {
				preCTrans.done = true
			}
		}
		p.removeTransaction(trans)
		tracing.EndTask(trans.id, p.cache)
	}

	// Process prefetch requests
	for _, trans := range prefetchRequests {
		// Mark prefetch as done so it can be cleaned up properly
		trans.done = true
		p.removeTransaction(trans)
		tracing.EndTask(trans.id, p.cache)
	}
}

func (p *bottomParser) findTransactionByWriteToBottomID(
	id string,
) *transaction {
	for _, trans := range p.cache.postCoalesceTransactions {
		if trans.writeToBottom != nil && trans.writeToBottom.ID == id {
			return trans
		}
	}
	return nil
}

func (p *bottomParser) findTransactionByReadToBottomID(
	id string,
) *transaction {
	for _, trans := range p.cache.postCoalesceTransactions {
		if trans.readToBottom != nil && trans.readToBottom.ID == id {
			return trans
		}
	}
	return nil
}

func (p *bottomParser) removeTransaction(trans *transaction) {
	for i, t := range p.cache.postCoalesceTransactions {
		if t == trans {
			p.cache.postCoalesceTransactions = append(
				(p.cache.postCoalesceTransactions)[:i],
				(p.cache.postCoalesceTransactions)[i+1:]...)
			return
		}
	}
}

func (p *bottomParser) getBankBuf(block *cache.Block) sim.Buffer {
	numWaysPerSet := p.cache.wayAssociativity
	blockID := block.SetID*numWaysPerSet + block.WayID
	bankID := blockID % len(p.cache.bankBufs)
	return p.cache.bankBufs[bankID]
}
