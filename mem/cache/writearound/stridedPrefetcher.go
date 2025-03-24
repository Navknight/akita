package writearound

import (
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
	return &StridePrefetcher{
		cache:                cache,
		degree:               degree,
		accessHistory:        make(map[vm.PID]map[uint64]*strideInfo),
		maxTrackedPages:      16,
		log2PageSize:         12, // 4KB pages
		enabled:              true,
		prefetchHits:         0,
		prefetchMisses:       0,
		totalPrefetches:      0,
		successfulPrefetches: 0,
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

	if pidMap, ok := p.accessHistory[pid]; ok {
		if info, ok := pidMap[pageAddr]; ok {
			if info.confidence >= info.prefetchThreshold && info.stride != 0 {
				madeProgress := false

				nextAddr := lineAddr + uint64(info.stride)
				for i := 0; i < p.degree; i++ {
					nextPageAddr := nextAddr >> uint64(p.log2PageSize)
					if (nextPageAddr == pageAddr || nextPageAddr == pageAddr+1) &&
						nextAddr != lineAddr {
						if p.issuePrefetch(now, pid, nextAddr) {
							madeProgress = true
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
	p.totalPrefetches++

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize

	if p.cache.directory.Lookup(pid, lineAddr) != nil {
		return false
	}

	if p.cache.mshr.Query(pid, lineAddr) != nil {
		return false
	}

	if p.cache.mshr.IsFull() {
		return false
	}

	victim := p.cache.directory.FindVictim(lineAddr)
	if victim == nil || victim.IsLocked || victim.ReadCount > 0 {
		return false
	}

	readToBottom := mem.ReadReqBuilder{}.
		WithSendTime(now).
		WithSrc(p.cache.bottomPort).
		WithDst(p.cache.lowModuleFinder.Find(lineAddr)).
		WithAddress(lineAddr).
		WithPID(pid).
		WithByteSize(blockSize).
		Build()

	err := p.cache.bottomPort.Send(readToBottom)
	if err != nil {
		return false
	}

	if victim.WasPrefetched {
		p.prefetchMisses++
	}

	victim.WasPrefetched = true
	p.successfulPrefetches++

	prefetchTrans := &transaction{
		id:           sim.GetIDGenerator().Generate(),
		readToBottom: readToBottom,
		read:         readToBottom,
		block:        victim,
	}

	mshrEntry := p.cache.mshr.Add(pid, lineAddr)
	mshrEntry.Requests = append(mshrEntry.Requests, prefetchTrans)
	mshrEntry.ReadReq = readToBottom
	mshrEntry.Block = victim

	victim.Tag = lineAddr
	victim.PID = pid
	victim.IsValid = true
	victim.IsLocked = true
	p.cache.directory.Visit(victim)

	p.cache.postCoalesceTransactions = append(p.cache.postCoalesceTransactions, prefetchTrans)

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
