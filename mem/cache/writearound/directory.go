package writearound

import (
	"log"

	"github.com/sarchlab/akita/v3/mem/cache"
	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/pipelining"
	"github.com/sarchlab/akita/v3/sim"
	"github.com/sarchlab/akita/v3/tracing"
)

type dirPipelineItem struct {
	trans *transaction
}

func (i dirPipelineItem) TaskID() string {
	return i.trans.id + "_dir_pipeline"
}

type directory struct {
	cache *Cache

	pipeline pipelining.Pipeline
	buf      sim.Buffer
}

func (d *directory) Tick(now sim.VTimeInSec) (madeProgress bool) {
	for i := 0; i < d.cache.numReqPerCycle; i++ {
		if !d.pipeline.CanAccept() {
			break
		}

		item := d.cache.dirBuf.Peek()
		if item == nil {
			break
		}

		trans := item.(*transaction)
		d.pipeline.Accept(now, dirPipelineItem{trans})
		d.cache.dirBuf.Pop()

		madeProgress = true
	}

	madeProgress = d.pipeline.Tick(now) || madeProgress

	for i := 0; i < d.cache.numReqPerCycle; i++ {
		item := d.buf.Peek()
		if item == nil {
			break
		}

		trans := item.(dirPipelineItem).trans

		if trans.read != nil {
			madeProgress = d.processRead(now, trans) || madeProgress
			continue
		}

		madeProgress = d.processWrite(now, trans) || madeProgress
	}

	return madeProgress
}

func (d *directory) processRead(now sim.VTimeInSec, trans *transaction) bool {
	read := trans.read
	addr := read.Address
	pid := read.PID
	blockSize := uint64(1 << d.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize

	// Record access for prefetcher
	if d.cache.Prefetcher != nil {
		d.cache.Prefetcher.RecordAccess(pid, addr)
	}

	mshrEntry := d.cache.mshr.Query(pid, cacheLineID)
	if mshrEntry != nil {
		return d.processMSHRHit(now, trans, mshrEntry)
	}

	block := d.cache.directory.Lookup(pid, cacheLineID)
	if block != nil && block.IsValid {
		return d.processReadHit(now, trans, block)
	}

	// Try prefetching on read miss
	if d.cache.Prefetcher != nil {
		d.cache.Prefetcher.TryPrefetch(now, pid, addr)
	}

	return d.processReadMiss(now, trans)
}

func (d *directory) processMSHRHit(
	now sim.VTimeInSec,
	trans *transaction,
	mshrEntry *cache.MSHREntry,
) bool {
	// Mark as demand request if it came from a higher level component
	if len(trans.preCoalesceTransactions) > 0 {
		trans.isDemandRequest = true

		// Check if any transaction in this MSHR is a prefetch
		for _, req := range mshrEntry.Requests {
			if prefetchTrans, ok := req.(*transaction); ok && prefetchTrans.isPrefetch {
				// We have a demand request hitting on a prefetch - record this as a prefetch hit
				if d.cache.Prefetcher != nil {
					// This assertion verifies the prefetch was for the right address
					if prefetchTrans.read != nil && prefetchTrans.read.Address != trans.Address() {
						log.Printf("Warning: Demand request for 0x%x hit MSHR with prefetch for 0x%x",
							trans.Address(), prefetchTrans.read.Address)
					}
					d.cache.Prefetcher.RecordPrefetchHit()
				}
				break
			}
		}
	}

	mshrEntry.Requests = append(mshrEntry.Requests, trans)

	if trans.read != nil {
		tracing.AddTaskStep(trans.id, d.cache, "read-mshr-hit")
	} else {
		tracing.AddTaskStep(trans.id, d.cache, "write-mshr-hit")
	}

	d.buf.Pop()
	return true
}

func (d *directory) processReadHit(
	now sim.VTimeInSec,
	trans *transaction,
	block *cache.Block,
) bool {
	if block.IsLocked {
		return false
	}

	bankBuf := d.getBankBuf(block)
	if !bankBuf.CanPush() {
		return false
	}

	if block.WasPrefetched && d.cache.Prefetcher != nil {
		// Assert block is valid and has correct address
		if !block.IsValid {
			panic("Prefetched block marked as hit but not valid")
		}
		if block.Tag != (trans.Address()/uint64(1<<d.cache.log2BlockSize))*uint64(1<<d.cache.log2BlockSize) {
			log.Printf("Warning: Prefetched block tag mismatch: expected 0x%x, got 0x%x",
				(trans.Address()/uint64(1<<d.cache.log2BlockSize))*uint64(1<<d.cache.log2BlockSize),
				block.Tag)
		}

		d.cache.Prefetcher.prefetchHits++
		block.WasPrefetched = false
	}

	trans.block = block
	trans.bankAction = bankActionReadHit
	block.ReadCount++
	d.cache.directory.Visit(block)
	bankBuf.Push(trans)

	d.buf.Pop()
	tracing.AddTaskStep(trans.id, d.cache, "read-hit")

	return true
}

func (d *directory) processReadMiss(
	now sim.VTimeInSec,
	trans *transaction,
) bool {
	read := trans.read
	addr := read.Address
	blockSize := uint64(1 << d.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize

	victim := d.cache.directory.FindVictim(cacheLineID)
	if victim.IsLocked || victim.ReadCount > 0 {
		return false
	}

	if victim.WasPrefetched && d.cache.Prefetcher != nil {
		victim.WasPrefetched = false
		d.cache.Prefetcher.prefetchMisses++
	}

	if d.cache.mshr.IsFull() {
		return false
	}

	if !d.fetchFromBottom(now, trans, victim) {
		return false
	}

	d.buf.Pop()
	tracing.AddTaskStep(trans.id, d.cache, "read-miss")

	return true
}

func (d *directory) processWrite(
	now sim.VTimeInSec,
	trans *transaction,
) bool {
	write := trans.write
	addr := write.Address
	pid := write.PID
	blockSize := uint64(1 << d.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize

	mshrEntry := d.cache.mshr.Query(pid, cacheLineID)
	if mshrEntry != nil {
		ok := d.writeBottom(now, trans)
		if ok {
			return d.processMSHRHit(now, trans, mshrEntry)
		}
		return false
	}
	if d.cache.Prefetcher != nil {
		d.cache.Prefetcher.RecordAccess(pid, addr)
	}
	block := d.cache.directory.Lookup(pid, cacheLineID)
	if block != nil && block.IsValid {
		return d.processWriteHit(now, trans, block)
	}

	return d.writeMiss(now, trans)
}

func (d *directory) writeMiss(
	now sim.VTimeInSec,
	trans *transaction,
) bool {
	if ok := d.writeBottom(now, trans); ok {
		tracing.AddTaskStep(trans.id, d.cache, "write-miss")
		d.buf.Pop()
		return true
	}

	return false
}

func (d *directory) writeBottom(now sim.VTimeInSec, trans *transaction) bool {
	write := trans.write
	addr := write.Address

	writeToBottom := mem.WriteReqBuilder{}.
		WithSendTime(now).
		WithSrc(d.cache.bottomPort).
		WithDst(d.cache.lowModuleFinder.Find(addr)).
		WithAddress(addr).
		WithPID(write.PID).
		WithData(write.Data).
		WithDirtyMask(write.DirtyMask).
		Build()

	err := d.cache.bottomPort.Send(writeToBottom)
	if err != nil {
		return false
	}

	trans.writeToBottom = writeToBottom

	tracing.TraceReqInitiate(writeToBottom, d.cache, trans.id)

	return true
}

func (d *directory) processWriteHit(
	now sim.VTimeInSec,
	trans *transaction,
	block *cache.Block,
) bool {
	if block.IsLocked || block.ReadCount > 0 {
		return false
	}

	bankBuf := d.getBankBuf(block)
	if !bankBuf.CanPush() {
		return false
	}

	if trans.writeToBottom == nil {
		ok := d.writeBottom(now, trans)
		if !ok {
			return false
		}
	}

	write := trans.write
	addr := write.Address
	blockSize := uint64(1 << d.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize
	block.IsLocked = true
	block.IsValid = true
	block.Tag = cacheLineID
	d.cache.directory.Visit(block)

	if block.WasPrefetched && d.cache.Prefetcher != nil {
		block.WasPrefetched = false
		d.cache.Prefetcher.prefetchHits++
	}

	trans.bankAction = bankActionWrite
	trans.block = block
	bankBuf.Push(trans)

	tracing.AddTaskStep(trans.id, d.cache, "write-hit")
	d.buf.Pop()

	return true
}

func (d *directory) fetchFromBottom(
	now sim.VTimeInSec,
	trans *transaction,
	victim *cache.Block,
) bool {
	addr := trans.Address()
	pid := trans.PID()
	blockSize := uint64(1 << d.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize

	bottomModule := d.cache.lowModuleFinder.Find(cacheLineID)
	readToBottom := mem.ReadReqBuilder{}.
		WithSendTime(now).
		WithSrc(d.cache.bottomPort).
		WithDst(bottomModule).
		WithAddress(cacheLineID).
		WithPID(pid).
		WithByteSize(blockSize).
		Build()
	err := d.cache.bottomPort.Send(readToBottom)
	if err != nil {
		return false
	}

	tracing.TraceReqInitiate(readToBottom, d.cache, trans.id)
	trans.readToBottom = readToBottom
	trans.block = victim

	mshrEntry := d.cache.mshr.Add(pid, cacheLineID)
	mshrEntry.Requests = append(mshrEntry.Requests, trans)
	mshrEntry.ReadReq = readToBottom
	mshrEntry.Block = victim

	victim.Tag = cacheLineID
	victim.PID = pid
	victim.IsValid = true
	victim.IsLocked = true
	d.cache.directory.Visit(victim)

	return true
}

func (d *directory) getBankBuf(block *cache.Block) sim.Buffer {
	numWaysPerSet := d.cache.directory.WayAssociativity()
	blockID := block.SetID*numWaysPerSet + block.WayID
	bankID := blockID % len(d.cache.bankBufs)
	return d.cache.bankBufs[bankID]
}
