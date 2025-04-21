package writearound

import (
	"fmt"

	"github.com/sarchlab/akita/v3/mem/cache"
	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/sim"
)

// A Cache is a customized L1 cache the for R9nano GPUs.
type Cache struct {
	*sim.TickingComponent

	topPort     sim.Port
	bottomPort  sim.Port
	controlPort sim.Port

	numReqPerCycle   int
	log2BlockSize    uint64
	storage          *mem.Storage
	directory        cache.Directory
	mshr             cache.MSHR
	bankLatency      int
	wayAssociativity int
	lowModuleFinder  mem.LowModuleFinder

	dirBuf   sim.Buffer
	bankBufs []sim.Buffer

	coalesceStage    *coalescer
	directoryStage   *directory
	bankStages       []*bankStage
	parseBottomStage *bottomParser
	respondStage     *respondStage
	controlStage     *controlStage

	Prefetcher          *StridePrefetcher
	lastPrefetcherCheck sim.VTimeInSec

	maxNumConcurrentTrans    int
	transactions             []*transaction
	postCoalesceTransactions []*transaction

	magicMode   bool
	dramStorage *mem.Storage

	BlockAccessDistribution map[uint64]uint64
	TotalEvictions          uint64
	CumulativeAccessCount   uint64

	isPaused bool
}

func (c *Cache) GetBlockAccessStats() map[string]interface{} {
	histogram := make(map[string]int)

	for i := uint64(0); i <= 10; i++ {
		histogram[fmt.Sprintf("%d", i)] = int(c.BlockAccessDistribution[i])
	}

	for start := uint64(11); start <= 100; start += 10 {
		end := start + 9
		bucketName := fmt.Sprintf("%d-%d", start, end)
		count := 0

		for i := start; i <= end; i++ {
			count += int(c.BlockAccessDistribution[i])
		}

		histogram[bucketName] = count
	}

	countOver100 := 0
	for i := uint64(101); i < 1000; i++ {
		if val, exists := c.BlockAccessDistribution[i]; exists {
			countOver100 += int(val)
		}
	}

	if countOver100 > 0 {
		histogram["100+"] = countOver100
	}

	return map[string]interface{}{
		"AccessHistogram": histogram,
		"TotalEvictions":  c.TotalEvictions,
	}
}

func (c *Cache) EnableAddressTracing(filename string) error {
	return c.coalesceStage.EnableAddressTracing(filename)
}

func (c *Cache) DisableAddressTracing() {
	c.coalesceStage.DisableAddressTracing()
}

// SetLowModuleFinder sets the finder that tells which remote port can serve
// the data on a certain address.
func (c *Cache) SetLowModuleFinder(lmf mem.LowModuleFinder) {
	c.lowModuleFinder = lmf
}

// Tick update the state of the cache
func (c *Cache) Tick(now sim.VTimeInSec) bool {
	madeProgress := false

	// Run prefetcher assertions periodically
	if c.Prefetcher != nil && now > c.lastPrefetcherCheck+0.0001 {
		c.Prefetcher.AssertInvariants()
		c.lastPrefetcherCheck = now
	}

	if !c.isPaused {
		madeProgress = c.runPipeline(now) || madeProgress
	}

	madeProgress = c.controlStage.Tick(now) || madeProgress

	return madeProgress
}

func (c *Cache) runPipeline(now sim.VTimeInSec) bool {
	madeProgress := false
	madeProgress = c.tickRespondStage(now) || madeProgress
	madeProgress = c.tickParseBottomStage(now) || madeProgress
	madeProgress = c.tickBankStage(now) || madeProgress
	madeProgress = c.tickDirectoryStage(now) || madeProgress
	madeProgress = c.tickCoalesceState(now) || madeProgress
	return madeProgress
}

func (c *Cache) tickRespondStage(now sim.VTimeInSec) bool {
	madeProgress := false
	for i := 0; i < c.numReqPerCycle; i++ {
		madeProgress = c.respondStage.Tick(now) || madeProgress
	}
	return madeProgress
}

func (c *Cache) tickParseBottomStage(now sim.VTimeInSec) bool {
	madeProgress := false

	for i := 0; i < c.numReqPerCycle; i++ {
		madeProgress = c.parseBottomStage.Tick(now) || madeProgress
	}

	return madeProgress
}

func (c *Cache) tickBankStage(now sim.VTimeInSec) bool {
	madeProgress := false
	for _, bs := range c.bankStages {
		madeProgress = bs.Tick(now) || madeProgress
	}
	return madeProgress
}

func (c *Cache) tickDirectoryStage(now sim.VTimeInSec) bool {
	return c.directoryStage.Tick(now)
}

func (c *Cache) tickCoalesceState(now sim.VTimeInSec) bool {
	madeProgress := false
	for i := 0; i < c.numReqPerCycle; i++ {
		madeProgress = c.coalesceStage.Tick(now) || madeProgress
	}
	return madeProgress
}
