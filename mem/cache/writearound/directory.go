package writearound

import (
	"github.com/sarchlab/akita/v3/mem/cache"
	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/mem/vm"
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
	d.cache.timeTaken[addr] = now
	mshrEntry := d.cache.mshr.Query(pid, cacheLineID)
	if mshrEntry != nil {
		return d.processMSHRHit(now, trans, mshrEntry)
	}

	block := d.cache.directory.Lookup(pid, cacheLineID)
	if block != nil && block.IsValid {
		return d.processReadHit(now, trans, block)
	}

	return d.processReadMiss(now, trans)
}

func (d *directory) processMSHRHit(
	now sim.VTimeInSec,
	trans *transaction,
	mshrEntry *cache.MSHREntry,
) bool {
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

	trans.block = block
	trans.bankAction = bankActionReadHit
	block.ReadCount++
	block.WasRead = true
	d.cache.directory.Visit(block)
	bankBuf.Push(trans)

	d.buf.Pop()
	tracing.AddTaskStep(trans.id, d.cache, "read-hit")
	if block.FromPrefetcher {
		tracing.AddTaskStep(trans.id, d.cache, "prefetch-hit")
	}

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

	if d.cache.mshr.IsFull() {
		return false
	}

	if !d.fetchFromBottom(now, trans, victim) {
		return false
	}

	d.buf.Pop()
	if victim.FromPrefetcher && !victim.WasRead {
		tracing.AddTaskStep(trans.id, d.cache, "prefetch-miss")
	}
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

	trans.bankAction = bankActionWrite
	trans.block = block
	bankBuf.Push(trans)

	tracing.AddTaskStep(trans.id, d.cache, "write-hit")
	d.buf.Pop()

	return true
}

type StridePrefetcher struct {
	lastAddrs      map[vm.PID]uint64
	strides        map[vm.PID]uint64
	confidence     map[vm.PID]uint64
	maxConfidence  int
	prefetchDegree int
	memoryLatency  int
}

func (d *directory) NewStridePrefetcher(latency ...int) *StridePrefetcher {
	defaultLatency := 100
	if len(latency) > 0 {
		defaultLatency = latency[0]
	}
	return &StridePrefetcher{
		lastAddrs:      make(map[vm.PID]uint64),
		strides:        make(map[vm.PID]uint64),
		confidence:     make(map[vm.PID]uint64),
		maxConfidence:  2,
		prefetchDegree: defaultLatency / 2,
		memoryLatency:  defaultLatency,
	}
}

func (sp *StridePrefetcher) UpdatePattern(pid vm.PID, addrs uint64) {
	lastAddrs, exists := sp.lastAddrs[pid]
	if exists {
		currentStride := int64(addrs) - int64(lastAddrs)

		if prevStride, ok := sp.strides[pid]; ok {
			if prevStride == uint64(currentStride) {
				sp.confidence[pid]++
			} else {
				sp.confidence[pid] = 1
				sp.strides[pid] = uint64(currentStride)
			}
		}
	}

	sp.lastAddrs[pid] = addrs
}

func (sp *StridePrefetcher) ShouldPrefetch(pid vm.PID) (bool, uint64) {
	confidence, exists := sp.confidence[pid]
	if !exists || confidence < uint64(sp.maxConfidence) {
		return false, 0
	}

	stride, exists := sp.strides[pid]
	if !exists {
		return false, 0
	}

	return true, stride
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
	victim.FromPrefetcher = false
	victim.WasRead = false
	d.cache.directory.Visit(victim)

	if d.cache.usePrefetcher && !d.cache.mshr.IsFull() {
		nextCacheLineID := cacheLineID + blockSize
		// uncomment for next line prefetching
		d.issuePrefetch(now, pid, nextCacheLineID, blockSize)

		// shouldPrefetch, stride := d.cache.prefetcher.ShouldPrefetch(pid)
		// if shouldPrefetch {
		// 	for i := 0; i <= d.cache.prefetcher.prefetchDegree; i++ {
		// 		nextCacheLineID = cacheLineID + blockSize + uint64(i)*stride
		// 		// nextLowModule = d.cache.lowModuleFinder.Find(nextCacheLineID)
		// 		d.issuePrefetch(now, pid, nextCacheLineID, blockSize)

		// 		if d.cache.mshr.IsFull() {
		// 			break
		// 		}
		// 	}
		// }
	}

	return true
}

func (d *directory) issuePrefetch(
	now sim.VTimeInSec,
	pid vm.PID,
	prefetchAddr uint64,
	blockSize uint64,
) {
	// Skip if address already in MSHR
	if d.cache.mshr.Query(pid, prefetchAddr) != nil {
		return
	}

	nextVictim := d.cache.directory.FindVictim(prefetchAddr)
	if nextVictim.IsLocked || nextVictim.ReadCount > 0 {
		return
	}

	nextLowModule := d.cache.lowModuleFinder.Find(prefetchAddr)
	prefetchReq := mem.ReadReqBuilder{}.
		WithSendTime(now).
		WithSrc(d.cache.bottomPort).
		WithDst(nextLowModule).
		WithAddress(prefetchAddr).
		WithPID(pid).
		WithByteSize(blockSize).
		WithPrefetcher().
		Build()
	if err := d.cache.bottomPort.Send(prefetchReq); err != nil {
		return
	}

	// Set up MSHR entry for prefetch
	mshrEntry := d.cache.mshr.Add(pid, prefetchAddr)
	prefetchTrans := &transaction{
		id:             sim.GetIDGenerator().Generate(),
		read:           prefetchReq,
		readToBottom:   prefetchReq,
		block:          nextVictim,
		fromPrefetcher: true,
	}

	mshrEntry.Requests = append(mshrEntry.Requests, prefetchTrans)
	mshrEntry.ReadReq = prefetchReq
	mshrEntry.Block = nextVictim

	nextVictim.Tag = prefetchAddr
	nextVictim.PID = pid
	nextVictim.IsValid = true
	nextVictim.IsLocked = true
	nextVictim.FromPrefetcher = true
	nextVictim.WasRead = false
	d.cache.directory.Visit(nextVictim)

	d.cache.postCoalesceTransactions = append(
		d.cache.postCoalesceTransactions,
		prefetchTrans,
	)
}

func (d *directory) getBankBuf(block *cache.Block) sim.Buffer {
	numWaysPerSet := d.cache.directory.WayAssociativity()
	blockID := block.SetID*numWaysPerSet + block.WayID
	bankID := blockID % len(d.cache.bankBufs)
	return d.cache.bankBufs[bankID]
}
