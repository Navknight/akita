package writearound

import (
	"fmt"
	"log"

	"github.com/sarchlab/akita/v3/mem/mem"
	"github.com/sarchlab/akita/v3/mem/vm"
	"github.com/sarchlab/akita/v3/sim"
	"github.com/sarchlab/akita/v3/tracing"
)

// StridePrefetcher is a component that can predict memory access pattern and
// prefetch data.
type StridePrefetcher struct {
	cache           *Cache
	degree          int
	accessHistory   map[vm.PID]map[uint64]*strideInfo
	maxTrackedPages int
	log2PageSize    int
	enabled         bool

	prefetchHits        uint64 // Prefetched blocks that were used
	prefetchMisses      uint64 // Prefetched blocks evicted without being used
	totalPrefetches     uint64 // Total prefetch attempts
	issuedPrefetches    uint64 // Prefetches actually sent to memory
	completedPrefetches uint64 // Prefetches that completed and brought data to cache

	// Debug counters for prefetch failures
	mshrCollisions       uint64 // Address already in MSHR
	blocksAlreadyInCache uint64 // Block already in cache
	mshrsFull            uint64 // MSHR was full
	lockedBlocks         uint64 // Victim block was locked
	sendErrors           uint64 // Error when sending request
}

type strideInfo struct {
	lastAddr          uint64
	prevAddr          uint64
	stride            int64
	confidence        int
	maxConfidence     int
	prefetchThreshold int
}

// NewStridePrefetcher creates a stride prefetcher for the given cache
func NewStridePrefetcher(cache *Cache, degree int) *StridePrefetcher {
	p := &StridePrefetcher{
		cache:               cache,
		degree:              degree,
		accessHistory:       make(map[vm.PID]map[uint64]*strideInfo),
		maxTrackedPages:     16,
		log2PageSize:        12, // 4KB pages
		enabled:             true,
		prefetchHits:        0,
		prefetchMisses:      0,
		totalPrefetches:     0,
		issuedPrefetches:    0,
		completedPrefetches: 0,
	}

	return p
}

// RecordAccess should be called when a memory address is accessed
func (p *StridePrefetcher) RecordAccess(pid vm.PID, addr uint64) {
	if !p.enabled {
		return
	}

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize

	if _, ok := p.accessHistory[pid]; !ok {
		p.accessHistory[pid] = make(map[uint64]*strideInfo)
	}

	pageAddr := lineAddr >> uint64(p.log2PageSize)
	info, ok := p.accessHistory[pid][pageAddr]

	if !ok {
		info = &strideInfo{
			lastAddr:          lineAddr,
			prevAddr:          0,
			stride:            0,
			confidence:        0,
			maxConfidence:     3,
			prefetchThreshold: 2,
		}

		if len(p.accessHistory[pid]) >= p.maxTrackedPages {
			for k := range p.accessHistory[pid] {
				delete(p.accessHistory[pid], k)
				break
			}
		}

		p.accessHistory[pid][pageAddr] = info
		return
	}

	currentStride := int64(lineAddr) - int64(info.lastAddr)

	if info.prevAddr != 0 {
		prevStride := int64(info.lastAddr) - int64(info.prevAddr)

		if (currentStride == prevStride) && currentStride != 0 {
			if info.confidence < info.maxConfidence {
				info.confidence++
			}
			info.stride = currentStride
		} else {
			if info.confidence >= 0 {
				info.confidence = 0
				info.stride = currentStride
			}
		}
	}

	info.prevAddr = info.lastAddr
	info.lastAddr = lineAddr
}

// TryPrefetch attempts to prefetch data based on stride patterns
func (p *StridePrefetcher) TryPrefetch(now sim.VTimeInSec, pid vm.PID, addr uint64) bool {
	if !p.enabled {
		return false
	}

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize
	pageAddr := lineAddr >> uint64(p.log2PageSize)

	if pidMap, ok := p.accessHistory[pid]; ok {
		if info, ok := pidMap[pageAddr]; ok {
			if info.confidence >= info.prefetchThreshold && info.stride != 0 {
				madeProgress := false
				prefetchCount := 0

				nextAddr := lineAddr + uint64(info.stride)
				for i := 0; i < p.degree; i++ {
					p.totalPrefetches++

					nextPageAddr := nextAddr >> uint64(p.log2PageSize)
					if (nextPageAddr == pageAddr || nextPageAddr == pageAddr+1) &&
						nextAddr != lineAddr {

						// Don't prefetch the triggering address itself
						if nextAddr == addr {
							nextAddr = nextAddr + uint64(info.stride)
							continue
						}

						if p.issuePrefetch(now, pid, nextAddr) {
							madeProgress = true
							prefetchCount++
						}
						nextAddr = nextAddr + uint64(info.stride)
					} else {
						break
					}
				}

				return madeProgress
			}
		}
	}

	return false
}

// issuePrefetch issues a prefetch request for the given address
func (p *StridePrefetcher) issuePrefetch(now sim.VTimeInSec, pid vm.PID, addr uint64) bool {
	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize

	// Check if already in cache
	if p.cache.directory.Lookup(pid, lineAddr) != nil {
		p.blocksAlreadyInCache++
		return false
	}

	// Check if already in MSHR
	if p.cache.mshr.Query(pid, lineAddr) != nil {
		p.mshrCollisions++
		return false
	}

	// Check if MSHR is full
	if p.cache.mshr.IsFull() {
		p.mshrsFull++
		return false
	}

	// Find victim block
	victim := p.cache.directory.FindVictim(lineAddr)
	if victim == nil {
		return false
	}

	if victim.IsLocked || victim.ReadCount > 0 {
		p.lockedBlocks++
		return false
	}

	// Track if the victim was a prefetched block that wasn't used
	if victim.WasPrefetched && victim.IsValid {
		p.prefetchMisses++
	}

	// Build request to lower memory level
	readToBottom := mem.ReadReqBuilder{}.
		WithSendTime(now).
		WithSrc(p.cache.bottomPort).
		WithDst(p.cache.lowModuleFinder.Find(lineAddr)).
		WithAddress(lineAddr).
		WithPID(pid).
		WithByteSize(blockSize).
		Build()

	// Send request
	err := p.cache.bottomPort.Send(readToBottom)
	if err != nil {
		p.sendErrors++
		return false
	}

	p.issuedPrefetches++

	// Mark block as prefetched
	victim.WasPrefetched = true

	// Create transaction for the prefetch
	prefetchTrans := &transaction{
		id:           sim.GetIDGenerator().Generate(),
		readToBottom: readToBottom,
		read:         readToBottom,
		block:        victim,
		isPrefetch:   true,
	}

	// Add to MSHR
	mshrEntry := p.cache.mshr.Add(pid, lineAddr)
	mshrEntry.Requests = append(mshrEntry.Requests, prefetchTrans)
	mshrEntry.ReadReq = readToBottom
	mshrEntry.Block = victim

	// Update victim block
	victim.Tag = lineAddr
	victim.PID = pid
	victim.IsValid = true
	victim.IsLocked = true
	p.cache.directory.Visit(victim)

	// Add to post-coalesce transactions
	p.cache.postCoalesceTransactions = append(p.cache.postCoalesceTransactions, prefetchTrans)

	// Create tracing task
	tracing.StartTaskWithSpecificLocation(
		prefetchTrans.id,
		readToBottom.ID,
		p.cache,
		"cache_prefetch",
		"prefetch",
		p.cache.Name()+".Prefetcher",
		nil,
	)
	tracing.TraceReqInitiate(readToBottom, p.cache, prefetchTrans.id)

	return true
}

// RecordPrefetchComplete records when a prefetch completes
func (p *StridePrefetcher) RecordPrefetchComplete() {
	p.completedPrefetches++
}

// GetPrefetchHits returns the number of prefetch hits
func (p *StridePrefetcher) GetPrefetchHits() uint64 {
	return p.prefetchHits
}

// GetPrefetchMisses returns the number of prefetch misses
func (p *StridePrefetcher) GetPrefetchMisses() uint64 {
	return p.prefetchMisses
}

// GetTotalPrefetches returns the total number of prefetches attempted
func (p *StridePrefetcher) GetTotalPrefetches() uint64 {
	return p.totalPrefetches
}

// GetSuccessfulPrefetches returns the number of prefetches actually issued
func (p *StridePrefetcher) GetSuccessfulPrefetches() uint64 {
	return p.issuedPrefetches
}

// GetCompletedPrefetches returns the number of completed prefetches
func (p *StridePrefetcher) GetCompletedPrefetches() uint64 {
	return p.completedPrefetches
}

// GetPrefetchAccuracy calculates the prefetch accuracy (hits divided by completed prefetches)
func (p *StridePrefetcher) GetPrefetchAccuracy() float64 {
	if p.completedPrefetches == 0 {
		return 0.0
	}
	return (float64(p.prefetchHits) / float64(p.completedPrefetches)) * 100.0
}

// GetDetailedStats returns detailed prefetcher statistics for reporting
func (p *StridePrefetcher) GetDetailedStats() map[string]interface{} {
	inCache := int64(p.completedPrefetches) - int64(p.prefetchHits) - int64(p.prefetchMisses)
	if inCache < 0 {
		panic("metrics are wrong")
	}

	return map[string]interface{}{
		"enabled":              p.enabled,
		"degree":               p.degree,
		"prefetch_hits":        p.prefetchHits,
		"prefetch_misses":      p.prefetchMisses,
		"total_prefetches":     p.totalPrefetches,
		"issued_prefetches":    p.issuedPrefetches,
		"completed_prefetches": p.completedPrefetches,
		"in_cache":             uint64(inCache),
		"prefetch_accuracy":    p.GetPrefetchAccuracy(),
		"mshr_collisions":      p.mshrCollisions,
		"blocks_in_cache":      p.blocksAlreadyInCache,
		"mshr_full_events":     p.mshrsFull,
		"locked_blocks":        p.lockedBlocks,
		"send_errors":          p.sendErrors,
	}
}

// AssertInvariants checks for and reports potential issues with the prefetcher
func (p *StridePrefetcher) AssertInvariants() {
	// This is kept simple for reliability
	if p.prefetchHits > p.completedPrefetches {
		log.Printf("Warning: More prefetch hits (%d) than completed prefetches (%d)",
			p.prefetchHits, p.completedPrefetches)
	}
}

// GenerateReport returns a formatted string with prefetcher statistics
func (p *StridePrefetcher) GenerateReport() string {
	inCache := int64(p.completedPrefetches) - int64(p.prefetchHits) - int64(p.prefetchMisses)
	if inCache < 0 {
		inCache = 0
	}

	report := fmt.Sprintf("Prefetcher Report for %s:\n", p.cache.Name())
	report += fmt.Sprintf("  Degree: %d\n", p.degree)
	report += fmt.Sprintf("  Prefetch Hits: %d\n", p.prefetchHits)
	report += fmt.Sprintf("  Prefetch Misses: %d\n", p.prefetchMisses)
	report += fmt.Sprintf("  Total Prefetch Attempts: %d\n", p.totalPrefetches)
	report += fmt.Sprintf("  Issued Prefetches: %d\n", p.issuedPrefetches)
	report += fmt.Sprintf("  Completed Prefetches: %d\n", p.completedPrefetches)
	report += fmt.Sprintf("  Blocks Still in Cache: %d\n", inCache)
	report += fmt.Sprintf("  Prefetch Accuracy: %.2f%%\n", p.GetPrefetchAccuracy())

	// Add success rate
	if p.totalPrefetches > 0 {
		report += fmt.Sprintf("  Issue Success Rate: %.2f%%\n",
			float64(p.issuedPrefetches)/float64(p.totalPrefetches)*100.0)
	}

	// Add utilization rate
	if p.completedPrefetches > 0 {
		report += fmt.Sprintf("  Prefetch Utilization: %.2f%%\n",
			float64(p.prefetchHits)/float64(p.completedPrefetches)*100.0)
	}

	report += fmt.Sprintf("  Failure Breakdown:\n")
	report += fmt.Sprintf("    MSHR Collisions: %d\n", p.mshrCollisions)
	report += fmt.Sprintf("    Already in Cache: %d\n", p.blocksAlreadyInCache)
	report += fmt.Sprintf("    MSHR Full: %d\n", p.mshrsFull)
	report += fmt.Sprintf("    Locked Blocks: %d\n", p.lockedBlocks)
	report += fmt.Sprintf("    Send Errors: %d\n", p.sendErrors)

	return report
}
