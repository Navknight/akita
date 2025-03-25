package writearound

import (
	"fmt"
	"log"
	"math"

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

	prefetchHits         uint64
	prefetchMisses       uint64
	totalPrefetches      uint64
	successfulPrefetches uint64
	completedPrefetcher  uint64

	distanceMultiplier    float64  // Multiplier for prefetch distance
	recentHits            []bool   // Recent hit/miss history
	adaptationInterval    uint64   // How often to adjust distance
	accessesSinceAdapt    uint64   // Counter for adaptation
	lastPrefetchAddresses []uint64 // Track last prefetched addresses for assertions

	// Debug counters
	mshrsFound           uint64 // Number of times an MSHR already existed for a prefetch
	blocksAlreadyInCache uint64 // Number of times a block was already in cache
	mshrsFull            uint64 // Number of times MSHR was full
	lockedBlocks         uint64 // Number of times victim block was locked
	sendErrors           uint64
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
		cache:                 cache,
		degree:                degree,
		accessHistory:         make(map[vm.PID]map[uint64]*strideInfo),
		maxTrackedPages:       16,
		log2PageSize:          12, // 4KB pages
		enabled:               true,
		prefetchHits:          0,
		prefetchMisses:        0,
		totalPrefetches:       0,
		successfulPrefetches:  0,
		completedPrefetcher:   0,
		distanceMultiplier:    1.0,
		recentHits:            make([]bool, 32),
		adaptationInterval:    1000,
		lastPrefetchAddresses: make([]uint64, 0, 100),
	}

	// Defensive assertion
	if p.cache == nil {
		panic("Cannot initialize prefetcher with nil cache")
	}

	return p
}

// Add adaptDistance method to adjust prefetch distance
func (p *StridePrefetcher) adaptDistance() {
	if len(p.recentHits) == 0 {
		return // Nothing to adapt yet
	}

	// Count recent hits
	hitCount := 0
	for _, hit := range p.recentHits {
		if hit {
			hitCount++
		}
	}

	// Calculate hit rate
	hitRate := float64(hitCount) / float64(len(p.recentHits))

	previousMultiplier := p.distanceMultiplier

	// Adapt distance multiplier based on hit rate
	if hitRate > 0.7 {
		// Good accuracy, be more aggressive
		p.distanceMultiplier = math.Min(p.distanceMultiplier*1.1, 3.0)
	} else if hitRate < 0.3 {
		// Poor accuracy, be more conservative
		p.distanceMultiplier = math.Max(p.distanceMultiplier*0.9, 0.5)
	}

	// Debug output if multiplier changed significantly
	if math.Abs(previousMultiplier-p.distanceMultiplier) > 0.1 {
		log.Printf("Prefetcher adjusted distance multiplier from %.2f to %.2f (hit rate: %.2f%%)",
			previousMultiplier, p.distanceMultiplier, hitRate*100)
	}
}

// Enable turns on the prefetcher
func (p *StridePrefetcher) Enable() {
	p.enabled = true
}

// Disable turns off the prefetcher
func (p *StridePrefetcher) Disable() {
	p.enabled = false
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

	// Assertion: stride should be a multiple of block size for aligned accesses
	if currentStride != 0 && currentStride%int64(blockSize) != 0 {
		log.Printf("Warning: Detected unaligned stride %d (not a multiple of block size %d)",
			currentStride, blockSize)
	}

	if info.prevAddr != 0 {
		prevStride := int64(info.lastAddr) - int64(info.prevAddr)

		if currentStride == prevStride && currentStride != 0 {
			if info.confidence < info.maxConfidence {
				info.confidence++
			}
			info.stride = currentStride
		} else {
			if info.confidence > 0 {
				info.confidence--
			}
			if info.confidence == 0 {
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

	// Track adaptation
	p.accessesSinceAdapt++
	if p.accessesSinceAdapt >= p.adaptationInterval {
		p.adaptDistance()
		p.accessesSinceAdapt = 0
	}

	if pidMap, ok := p.accessHistory[pid]; ok {
		if info, ok := pidMap[pageAddr]; ok {
			if info.confidence >= info.prefetchThreshold && info.stride != 0 {
				// Assert valid stride
				if info.stride == 0 {
					panic("Attempted to prefetch with zero stride despite confidence check")
				}

				madeProgress := false
				prefetchAttempts := 0
				prefetchSuccesses := 0

				// Calculate adaptive stride
				adaptiveStride := int64(float64(info.stride) * p.distanceMultiplier)

				// Ensure minimum stride is block size
				if math.Abs(float64(adaptiveStride)) < float64(blockSize) {
					if adaptiveStride < 0 {
						adaptiveStride = -int64(blockSize)
					} else {
						adaptiveStride = int64(blockSize)
					}
				}

				// Assert stride is valid
				if adaptiveStride == 0 {
					panic("Adaptive stride calculation resulted in zero stride")
				}

				nextAddr := lineAddr + uint64(adaptiveStride)
				for i := 0; i < p.degree; i++ {
					prefetchAttempts++

					nextPageAddr := nextAddr >> uint64(p.log2PageSize)
					if (nextPageAddr == pageAddr || nextPageAddr == pageAddr+1) &&
						nextAddr != lineAddr {

						// Don't prefetch the triggering address itself
						if nextAddr == addr {
							log.Printf("Warning: Skipping prefetch of trigger address 0x%x", addr)
							nextAddr = nextAddr + uint64(adaptiveStride)
							continue
						}

						if p.issuePrefetch(now, pid, nextAddr) {
							madeProgress = true
							prefetchSuccesses++
						}
						nextAddr = nextAddr + uint64(adaptiveStride)
					} else {
						break
					}
				}

				// Assert reasonable prefetch success ratio over time
				if prefetchAttempts > 0 && p.totalPrefetches > 100 {
					successRatio := float64(p.successfulPrefetches) / float64(p.totalPrefetches)
					if successRatio < 0.01 {
						log.Printf("Warning: Very low prefetch success ratio (%.2f%%). Check for resource constraints.",
							successRatio*100)
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
	p.totalPrefetches++

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize

	// Assert address alignment
	if lineAddr != addr/blockSize*blockSize {
		panic(fmt.Sprintf("Prefetch address 0x%x not aligned to block size %d",
			addr, blockSize))
	}

	// Check if already in cache
	if p.cache.directory.Lookup(pid, lineAddr) != nil {
		p.blocksAlreadyInCache++
		return false
	}

	// Check if already in MSHR
	if p.cache.mshr.Query(pid, lineAddr) != nil {
		p.mshrsFound++
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
		panic("FindVictim returned nil despite directory implementation")
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

	// Success tracking
	victim.WasPrefetched = true
	p.successfulPrefetches++

	// Create transaction for the prefetch
	prefetchTrans := &transaction{
		id:           sim.GetIDGenerator().Generate(),
		readToBottom: readToBottom,
		read:         readToBottom,
		block:        victim,
		isPrefetch:   true, // Mark as prefetch transaction
	}

	// Add to MSHR
	mshrEntry := p.cache.mshr.Add(pid, lineAddr)
	if mshrEntry == nil {
		panic("MSHR Add returned nil despite earlier IsFull check")
	}

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

	// Track this address for debugging
	p.lastPrefetchAddresses = append(p.lastPrefetchAddresses, lineAddr)
	if len(p.lastPrefetchAddresses) > 100 {
		p.lastPrefetchAddresses = p.lastPrefetchAddresses[1:]
	}

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

// New method to get detailed prefetcher statistics
func (p *StridePrefetcher) GetDetailedStats() map[string]interface{} {
	stats := map[string]interface{}{
		"enabled":                 p.enabled,
		"degree":                  p.degree,
		"prefetch_hits":           p.prefetchHits,
		"prefetch_misses":         p.prefetchMisses,
		"total_prefetches":        p.totalPrefetches,
		"successful_prefetches":   p.successfulPrefetches,
		"completed_prefetches":    p.completedPrefetcher,
		"distance_multiplier":     p.distanceMultiplier,
		"prefetch_accuracy":       p.GetPrefetchAccuracy(),
		"mshr_collisions":         p.mshrsFound,
		"blocks_already_in_cache": p.blocksAlreadyInCache,
		"mshr_full_events":        p.mshrsFull,
		"locked_block_events":     p.lockedBlocks,
		"send_errors":             p.sendErrors,
	}

	// Add last 5 prefetch addresses for debugging
	lastAddrs := make([]string, 0)
	for i := len(p.lastPrefetchAddresses) - 1; i >= 0 && i >= len(p.lastPrefetchAddresses)-5; i-- {
		lastAddrs = append(lastAddrs, fmt.Sprintf("0x%x", p.lastPrefetchAddresses[i]))
	}
	stats["last_prefetch_addresses"] = lastAddrs

	// Get page coverage stats
	pagesTracked := 0
	for _, pageMap := range p.accessHistory {
		pagesTracked += len(pageMap)
	}
	stats["pages_tracked"] = pagesTracked

	return stats
}

// Add periodic assertion check method
func (p *StridePrefetcher) AssertInvariants() {
	// Check for reasonable hit rates over time
	if p.prefetchHits+p.prefetchMisses > 1000 {
		hitRate := float64(p.prefetchHits) / float64(p.prefetchHits+p.prefetchMisses)
		if hitRate < 0.05 {
			log.Printf("Warning: Very low prefetch hit rate (%.2f%%). Consider adjusting prefetcher parameters.",
				hitRate*100)
		}
	}

	// Check for low-confidence prefetching
	if p.totalPrefetches > 0 {
		successRate := float64(p.successfulPrefetches) / float64(p.totalPrefetches)
		if successRate < 0.25 {
			log.Printf("Warning: Low prefetch issue success rate (%.2f%%). Check for resource constraints.",
				successRate*100)
		}
	}

	// Verify completion of prefetches
	if p.successfulPrefetches > 0 {
		completionRate := float64(p.completedPrefetcher) / float64(p.successfulPrefetches)
		if completionRate < 0.9 {
			log.Printf("Warning: Not all prefetches are completing (%.2f%% completion rate).",
				completionRate*100)
		}
	}
}

// RecordPrefetchHit should be called when a prefetched block is hit
func (p *StridePrefetcher) RecordPrefetchHit() {
	p.prefetchHits++
}

// RecordPrefetchComplete should be called when a prefetch request completes
func (p *StridePrefetcher) RecordPrefetchComplete() {
	p.completedPrefetcher++
}

// Cleanup should be called when the prefetcher is no longer needed
func (p *StridePrefetcher) Cleanup() {}

// GetPrefetchHits returns the number of prefetch hits
func (p *StridePrefetcher) GetPrefetchHits() uint64 {
	return p.prefetchHits
}

// GetPrefetchMisses returns the number of prefetch misses
func (p *StridePrefetcher) GetPrefetchMisses() uint64 {
	return p.prefetchMisses
}

// GetTotalPrefetches returns the total number of prefetches
func (p *StridePrefetcher) GetTotalPrefetches() uint64 {
	return p.totalPrefetches
}

// GetSuccessfulPrefetches returns the number of successful prefetches
func (p *StridePrefetcher) GetSuccessfulPrefetches() uint64 {
	return p.successfulPrefetches
}

// GetCompletedPrefetches returns the number of completed prefetches
func (p *StridePrefetcher) GetCompletedPrefetches() uint64 {
	return p.completedPrefetcher
}

// GetPrefetchAccuracy calculates the prefetch accuracy
func (p *StridePrefetcher) GetPrefetchAccuracy() float64 {
	if p.successfulPrefetches == 0 {
		return 0.0
	}
	return float64(p.prefetchHits) / float64(p.successfulPrefetches) * 100.0
}

// GetInCache returns the number of prefetched blocks in the cache
func (p *StridePrefetcher) GetInCache() float64 {
	return p.cache.directory.GetPrefetchedBlockCount()
}

func (p *StridePrefetcher) GenerateReport() string {
	report := fmt.Sprintf("Prefetcher Report for %s:\n", p.cache.Name())
	report += fmt.Sprintf("  Enabled: %v\n", p.enabled)
	report += fmt.Sprintf("  Degree: %d\n", p.degree)
	report += fmt.Sprintf("  Distance Multiplier: %.2f\n", p.distanceMultiplier)
	report += fmt.Sprintf("  Prefetch Hits: %d\n", p.prefetchHits)
	report += fmt.Sprintf("  Prefetch Misses: %d\n", p.prefetchMisses)
	report += fmt.Sprintf("  Total Prefetch Attempts: %d\n", p.totalPrefetches)
	report += fmt.Sprintf("  Successful Prefetches: %d\n", p.successfulPrefetches)
	report += fmt.Sprintf("  Completed Prefetches: %d\n", p.completedPrefetcher)
	report += fmt.Sprintf("  Prefetch Accuracy: %.2f%%\n", p.GetPrefetchAccuracy())
	report += fmt.Sprintf("  MSHR Collisions: %d\n", p.mshrsFound)
	report += fmt.Sprintf("  Blocks Already in Cache: %d\n", p.blocksAlreadyInCache)
	report += fmt.Sprintf("  MSHR Full Events: %d\n", p.mshrsFull)
	report += fmt.Sprintf("  Locked Block Events: %d\n", p.lockedBlocks)
	report += fmt.Sprintf("  Send Errors: %d\n", p.sendErrors)
	return report
}
