package writearound

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/sim"
	"github.com/sarchlab/akita/v3/tracing"
)

type coalescer struct {
	cache       *Cache
	toCoalesce  []*transaction
	traceFile   *os.File
	traceWriter *log.Logger
	traceMutex  sync.Mutex

	traceBuffer   []string
	lastFlushTime time.Time
	flushInterval time.Duration
}

func (c *coalescer) Reset() {
	c.toCoalesce = nil
}

func (c *coalescer) Tick(now sim.VTimeInSec) bool {
	req := c.cache.topPort.Peek()
	if req == nil {
		return false
	}

	if c.traceWriter != nil && time.Since(c.lastFlushTime) > c.flushInterval {
		c.flushTraceBuffer()
	}

	return c.processReq(now, req.(mem.AccessReq))
}

func (c *coalescer) EnableAddressTracing(filename string) error {
	var err error
	c.traceFile, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening file '%s': %v\n", filename, err)
		return err
	}

	c.traceWriter = log.New(c.traceFile, "", 0)
	fi, err := c.traceFile.Stat()
	if err != nil {
		return err
	}

	if fi.Size() == 0 {
		c.traceWriter.Println("Time, Address, Size, IsRead, Source")
	}

	c.traceBuffer = make([]string, 0, 1000)
	c.lastFlushTime = time.Now()
	c.flushInterval = 5 * time.Second

	return nil
}

func (c *coalescer) DisableAddressTracing() {
	if c.traceFile != nil {
		c.flushTraceBuffer()
		c.traceFile.Close()
		c.traceFile = nil
		c.traceWriter = nil
		c.traceBuffer = nil
	}
}

func (c *coalescer) TraceAddress(now sim.VTimeInSec, req mem.AccessReq) {
	if c.traceWriter == nil {
		return
	}

	isRead := 0
	if _, ok := req.(*mem.ReadReq); ok {
		isRead = 1
	}

	sourceName := "unknown"
	if req.Meta().Src != nil {
		sourceName = req.Meta().Src.Name()
	}

	c.traceBuffer = append(c.traceBuffer, fmt.Sprintf(
		"%f,0x%x,%d,%d,%s\n",
		float64(now),
		req.GetAddress(),
		req.GetByteSize(),
		isRead,
		sourceName,
	))
}

func (c *coalescer) flushTraceBuffer() {
	if len(c.traceBuffer) == 0 {
		c.lastFlushTime = time.Now()
		return
	}
	c.traceMutex.Lock()
	defer c.traceMutex.Unlock()

	c.traceWriter.Println(strings.Join(c.traceBuffer, "\n"))

	c.traceBuffer = c.traceBuffer[:0]
	c.lastFlushTime = time.Now()
}

func (c *coalescer) processReq(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	if len(c.cache.transactions) >= c.cache.maxNumConcurrentTrans {
		return false
	}

	c.TraceAddress(now, req)

	switch item := req.(type) {
	case *mem.GL0InvalidateReq:
		return c.processGL0InvalidateReq(item, now)
	}

	if c.isReqLastInWave(req) {
		if len(c.toCoalesce) == 0 || c.canReqCoalesce(req) {
			return c.processReqLastInWaveCoalescable(now, req)
		}
		return c.processReqLastInWaveNoncoalescable(now, req)
	}

	if len(c.toCoalesce) == 0 || c.canReqCoalesce(req) {
		return c.processReqCoalescable(now, req)
	}
	return c.processReqNoncoalescable(now, req)
}

func (c *coalescer) processGL0InvalidateReq(
	req *mem.GL0InvalidateReq,
	now sim.VTimeInSec,
) bool {
	c.cache.directory.Reset()

	rsp := mem.GL0InvalidateRspBuilder{}.
		WithPID(req.GetPID()).
		WithSrc(c.cache.topPort).
		WithDst(req.Meta().Src).
		WithSendTime(now).
		Build()

	err := c.cache.topPort.Send(rsp)
	if err == nil {
		c.cache.topPort.Retrieve(now)
		return true
	}
	return false
}

func (c *coalescer) processReqCoalescable(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	trans := c.createTransaction(req, now)
	c.toCoalesce = append(c.toCoalesce, trans)
	c.cache.transactions = append(c.cache.transactions, trans)
	c.cache.topPort.Retrieve(now)

	tracing.TraceReqReceive(req, c.cache)
	return true
}

func (c *coalescer) processReqNoncoalescable(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	if !c.cache.dirBuf.CanPush() {
		return false
	}

	c.coalesceAndSend(now)

	trans := c.createTransaction(req, now)
	c.toCoalesce = append(c.toCoalesce, trans)
	c.cache.transactions = append(c.cache.transactions, trans)
	c.cache.topPort.Retrieve(now)

	tracing.TraceReqReceive(req, c.cache)
	return true
}

func (c *coalescer) processReqLastInWaveCoalescable(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	if !c.cache.dirBuf.CanPush() {
		return false
	}

	trans := c.createTransaction(req, now)
	c.toCoalesce = append(c.toCoalesce, trans)
	c.cache.transactions = append(c.cache.transactions, trans)
	c.coalesceAndSend(now)
	c.cache.topPort.Retrieve(now)

	tracing.TraceReqReceive(req, c.cache)
	return true
}

func (c *coalescer) processReqLastInWaveNoncoalescable(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	if !c.cache.dirBuf.CanPush() {
		return false
	}
	c.coalesceAndSend(now)

	if !c.cache.dirBuf.CanPush() {
		return true
	}

	trans := c.createTransaction(req, now)
	c.toCoalesce = append(c.toCoalesce, trans)
	c.cache.transactions = append(c.cache.transactions, trans)
	c.coalesceAndSend(now)
	c.cache.topPort.Retrieve(now)

	tracing.TraceReqReceive(req, c.cache)
	return true
}

func (c *coalescer) createTransaction(req mem.AccessReq, now sim.VTimeInSec) *transaction {
	switch req := req.(type) {
	case *mem.ReadReq:
		t := &transaction{
			read: req,
		}
		return t
	case *mem.WriteReq:
		t := &transaction{
			write: req,
		}
		return t
	default:
		log.Panicf("cannot process request of type %s\n", reflect.TypeOf(req))
		return nil
	}
}

func (c *coalescer) isReqLastInWave(req mem.AccessReq) bool {
	switch req := req.(type) {
	case *mem.ReadReq:
		return !req.CanWaitForCoalesce
	case *mem.WriteReq:
		return !req.CanWaitForCoalesce
	default:
		panic("unknown type")
	}
}

func (c *coalescer) canReqCoalesce(req mem.AccessReq) bool {
	blockSize := uint64(1 << c.cache.log2BlockSize)
	return req.GetAddress()/blockSize == c.toCoalesce[0].Address()/blockSize
}

func (c *coalescer) coalesceAndSend(now sim.VTimeInSec) bool {
	var trans *transaction
	if c.toCoalesce[0].read != nil {
		trans = c.coalesceRead()
		tracing.StartTaskWithSpecificLocation(trans.id,
			tracing.MsgIDAtReceiver(c.toCoalesce[0].read, c.cache),
			c.cache, "cache_transaction", "read",
			c.cache.Name()+".Local",
			nil)
	} else {
		trans = c.coalesceWrite()
		tracing.StartTaskWithSpecificLocation(trans.id,
			tracing.MsgIDAtReceiver(c.toCoalesce[0].write, c.cache),
			c.cache, "cache_transaction", "write",
			c.cache.Name()+".Local",
			nil)
	}
	c.cache.dirBuf.Push(trans)
	c.cache.postCoalesceTransactions =
		append(c.cache.postCoalesceTransactions, trans)
	c.toCoalesce = nil

	return true
}

func (c *coalescer) coalesceRead() *transaction {
	blockSize := uint64(1 << c.cache.log2BlockSize)
	cachelineID := c.toCoalesce[0].Address() / blockSize * blockSize
	coalescedRead := mem.ReadReqBuilder{}.
		WithAddress(cachelineID).
		WithByteSize(blockSize).
		WithPID(c.toCoalesce[0].PID()).
		Build()
	return &transaction{
		id:                      sim.GetIDGenerator().Generate(),
		read:                    coalescedRead,
		preCoalesceTransactions: c.toCoalesce,
	}
}

func (c *coalescer) coalesceWrite() *transaction {
	blockSize := uint64(1 << c.cache.log2BlockSize)
	cachelineID := c.toCoalesce[0].Address() / blockSize * blockSize
	write := mem.WriteReqBuilder{}.
		WithAddress(cachelineID).
		WithPID(c.toCoalesce[0].PID()).
		WithData(make([]byte, blockSize)).
		WithDirtyMask(make([]bool, blockSize)).
		Build()

	for _, t := range c.toCoalesce {
		w := t.write
		offset := int(w.Address - cachelineID)
		for i := 0; i < len(w.Data); i++ {
			if w.DirtyMask == nil || w.DirtyMask[i] {
				write.Data[i+offset] = w.Data[i]
				write.DirtyMask[i+offset] = true
			}
		}
	}
	return &transaction{
		id:                      sim.GetIDGenerator().Generate(),
		write:                   write,
		preCoalesceTransactions: c.toCoalesce,
	}
}
