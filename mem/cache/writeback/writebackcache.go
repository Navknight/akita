package writeback

import (
	"github.com/sarchlab/akita/v3/mem/cache"
	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/sim"
	"fmt"
)

type cacheState int

const (
	cacheStateInvalid cacheState = iota
	cacheStateRunning
	cacheStatePreFlushing
	cacheStateFlushing
	cacheStatePaused
)

// A Cache in the writeback package is a cache that performs the write-back policy.
type Cache struct {
	*sim.TickingComponent

	topPort     sim.Port
	bottomPort  sim.Port
	controlPort sim.Port

	dirStageBuffer           sim.Buffer
	dirToBankBuffers         []sim.Buffer
	writeBufferToBankBuffers []sim.Buffer
	mshrStageBuffer          sim.Buffer
	writeBufferBuffer        sim.Buffer

	topSender         sim.BufferedSender
	bottomSender      sim.BufferedSender
	controlPortSender sim.BufferedSender

	topParser   *topParser
	writeBuffer *writeBufferStage
	dirStage    *directoryStage
	bankStages  []*bankStage
	mshrStage   *mshrStage
	flusher     *flusher

	storage         *mem.Storage
	lowModuleFinder mem.LowModuleFinder
	directory       cache.Directory
	mshr            cache.MSHR
	log2BlockSize   uint64
	numReqPerCycle  int

	state                cacheState
	inFlightTransactions []*transaction
	evictingList         map[uint64]bool

	BlockAccessDistribution map[uint64]uint64  // Maps access count -> number of blocks with that count
	TotalEvictions          uint64             // Total number of evictions
	CumulativeAccessCount   uint64						// Sum of access counts all evicted blocks 
}

// SetLowModuleFinder sets the LowModuleFinder used by the cache.
func (c *Cache) SetLowModuleFinder(lmf mem.LowModuleFinder) {
	c.lowModuleFinder = lmf
}

// Tick updates the internal states of the Cache.
func (c *Cache) Tick(now sim.VTimeInSec) bool {
	madeProgress := false

	madeProgress = c.controlPortSender.Tick(now) || madeProgress

	if c.state != cacheStatePaused {
		madeProgress = c.runPipeline(now) || madeProgress
	}

	madeProgress = c.flusher.Tick(now) || madeProgress

	return madeProgress
}

func (c *Cache) runPipeline(now sim.VTimeInSec) bool {
	madeProgress := false

	madeProgress = c.runStage(now, c.topSender) || madeProgress
	madeProgress = c.runStage(now, c.bottomSender) || madeProgress
	madeProgress = c.runStage(now, c.mshrStage) || madeProgress

	for _, bs := range c.bankStages {
		madeProgress = bs.Tick(now) || madeProgress
	}

	madeProgress = c.runStage(now, c.writeBuffer) || madeProgress
	madeProgress = c.runStage(now, c.dirStage) || madeProgress
	madeProgress = c.runStage(now, c.topParser) || madeProgress

	return madeProgress
}

func (c *Cache) runStage(now sim.VTimeInSec, stage sim.Ticker) bool {
	madeProgress := false
	for i := 0; i < c.numReqPerCycle; i++ {
		madeProgress = stage.Tick(now) || madeProgress
	}
	return madeProgress
}

func (c *Cache) discardInflightTransactions(now sim.VTimeInSec) {
	sets := c.directory.GetSets()
	for _, set := range sets {
		for _, block := range set.Blocks {
			block.ReadCount = 0
			block.IsLocked = false
		}
	}

	c.dirStage.Reset(now)
	for _, bs := range c.bankStages {
		bs.Reset(now)
	}
	c.mshrStage.Reset(now)
	c.writeBuffer.Reset(now)

	clearPort(c.topPort, now)

	c.topSender.Clear()

	// for _, t := range c.inFlightTransactions {
	// 	fmt.Printf("%.10f, %s, transaction %s discarded due to flushing\n",
	// 		now, c.Name(), t.id)
	// }

	c.inFlightTransactions = nil
}

// GetBlockAccessStats returns statistics about block accesses before eviction
// in the form of a histogram
func (c *Cache) GetBlockAccessStats() map[string]interface{} {
	histogram := make(map[string]int)
	
	// Individual buckets for 0-10
	for i := uint64(0); i <= 10; i++ {
		histogram[fmt.Sprintf("%d", i)] = int(c.BlockAccessDistribution[i])
	}
	
	// Grouped buckets for ranges
	for start := uint64(11); start <= 100; start += 10 {
		end := start + 9
		bucketName := fmt.Sprintf("%d-%d", start, end)
		count := 0
		
		for i := start; i <= end; i++ {
			count += int(c.BlockAccessDistribution[i])
		}
		
		histogram[bucketName] = count
	}
	
	// Bucket for >100
	countOver100 := 0
	for i := uint64(101); i < 1000; i++ {
		if val, exists := c.BlockAccessDistribution[i]; exists {
			countOver100 += int(val)
		}
	}
	if countOver100 > 0 {
		histogram[">100"] = countOver100
	}
	
	return map[string]interface{}{
		"AccessHistogram": histogram,
		"TotalEvictions":  c.TotalEvictions,
	}
}
