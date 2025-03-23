package writearound

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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
	logger          *log.Logger
	logFile         *os.File

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
	// Create logs directory if it doesn't exist
	logsDir := "prefetcher_logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		panic(err)
	}

	// Create a unique log file with timestamp and degree
	timestamp := time.Now().Format("20060102_150405")
	logFileName := filepath.Join(logsDir, fmt.Sprintf("stride_prefetcher_deg%d_%s.log", degree, timestamp))

	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	logger := log.New(logFile, "", log.LstdFlags)
	logger.Printf("NEW_PREFETCHER Degree:%d MaxTrackedPages:16 PageSize:4KB", degree)

	return &StridePrefetcher{
		cache:                cache,
		degree:               degree,
		accessHistory:        make(map[vm.PID]map[uint64]*strideInfo),
		maxTrackedPages:      16,
		log2PageSize:         12, // 4KB pages
		enabled:              true,
		logger:               logger,
		logFile:              logFile,
		prefetchHits:         0,
		prefetchMisses:       0,
		totalPrefetches:      0,
		successfulPrefetches: 0,
	}
}

// Enable turns on the prefetcher
func (p *StridePrefetcher) Enable() {
	p.enabled = true
	p.logger.Println("ENABLED")
}

// Disable turns off the prefetcher
func (p *StridePrefetcher) Disable() {
	p.enabled = false
	p.logger.Println("DISABLED")
}

// RecordAccess should be called when a memory address is accessed
func (p *StridePrefetcher) RecordAccess(pid vm.PID, addr uint64) {
	p.logger.Printf("RA PID:%d ADDR:%x", pid, addr)
	if !p.enabled {
		p.logger.Println("DISABLED_SKIP_RA")
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
		p.logger.Printf("NEW_SI PID:%d PAGE:%x", pid, pageAddr)
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
			p.logger.Printf("STRIDE_MATCH CONF:%d STRIDE:%d", info.confidence, info.stride)
		} else {
			// Different stride, decrease confidence
			if info.confidence > 0 {
				info.confidence--
			}
			// Only update stride if confidence is reset
			if info.confidence == 0 {
				info.stride = currentStride
			}
			p.logger.Printf("STRIDE_MISMATCH CONF:%d STRIDE:%d", info.confidence, info.stride)
		}
	}

	// Update address history
	info.prevAddr = info.lastAddr
	info.lastAddr = lineAddr
	p.logger.Printf("UPDATE_ADDR LAST:%x PREV:%x", info.lastAddr, info.prevAddr)
}

// TryPrefetch attempts to prefetch data based on stride patterns
func (p *StridePrefetcher) TryPrefetch(now sim.VTimeInSec, pid vm.PID, addr uint64) bool {
	p.logger.Printf("TP PID:%d ADDR:%x", pid, addr)
	if !p.enabled {
		p.logger.Println("DISABLED_SKIP_TP")
		return false
	}

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize
	pageAddr := lineAddr >> uint64(p.log2PageSize)

	// Check if we have stride info with sufficient confidence
	if pidMap, ok := p.accessHistory[pid]; ok {
		if info, ok := pidMap[pageAddr]; ok {
			if info.confidence >= info.prefetchThreshold && info.stride != 0 {
				madeProgress := false

				// Generate prefetch addresses
				nextAddr := lineAddr + uint64(info.stride)
				for i := 0; i < p.degree; i++ {
					// Check if the address would be in the same or adjacent page
					nextPageAddr := nextAddr >> uint64(p.log2PageSize)
					if (nextPageAddr == pageAddr || nextPageAddr == pageAddr+1) &&
						nextAddr != lineAddr {
						// Attempt to prefetch this address
						if p.issuePrefetch(now, pid, nextAddr) {
							madeProgress = true
							p.logger.Printf("PREFETCH_ISSUED ADDR:%x DEGREE:%d", nextAddr, i+1)
						}
						nextAddr = nextAddr + uint64(info.stride)
					} else {
						p.logger.Printf("PAGE_BOUNDARY_BREAK ADDR:%x PAGE:%x NEXT_PAGE:%x",
							nextAddr, pageAddr, nextPageAddr)
						break
					}
				}

				return madeProgress
			}
		}
	}

	p.logger.Println("NO_CONFIDENCE_SKIP_PREFETCH")
	return false
}

// issuePrefetch issues a prefetch request for the given address
func (p *StridePrefetcher) issuePrefetch(now sim.VTimeInSec, pid vm.PID, addr uint64) bool {
	p.totalPrefetches++

	p.logger.Printf("ISSUE_PREFETCH PID:%d ADDR:%x", pid, addr)

	blockSize := uint64(1 << p.cache.log2BlockSize)
	lineAddr := addr / blockSize * blockSize

	// Check if the address is already in the cache
	if p.cache.directory.Lookup(pid, lineAddr) != nil {
		p.logger.Printf("CACHE_HIT ADDR:%x", lineAddr)
		return false
	}

	// Check if there's already an MSHR entry for this address
	if p.cache.mshr.Query(pid, lineAddr) != nil {
		p.logger.Printf("MSHR_HIT ADDR:%x", lineAddr)
		return false
	}

	// Check if the MSHR is full
	if p.cache.mshr.IsFull() {
		p.logger.Println("MSHR_FULL")
		return false
	}

	// Find a victim block
	victim := p.cache.directory.FindVictim(lineAddr)
	if victim == nil || victim.IsLocked || victim.ReadCount > 0 {
		p.logger.Println("NO_VICTIM")
		return false
	}

	// Create a read request for the prefetch
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
		p.logger.Printf("SEND_FAIL ADDR:%x ERR:%v", lineAddr, err)
		return false
	} else {
		p.logger.Printf("SEND_SUCCESS ADDR:%x", lineAddr)
	}

	if victim.WasPrefetched {
		p.prefetchMisses++
	}

	victim.WasPrefetched = true
	p.successfulPrefetches++

	// Create a transaction for this prefetch
	prefetchTrans := &transaction{
		id:           sim.GetIDGenerator().Generate(),
		readToBottom: readToBottom,
		read:         readToBottom,
		block:        victim,
	}

	// Create an MSHR entry
	mshrEntry := p.cache.mshr.Add(pid, lineAddr)
	mshrEntry.Requests = append(mshrEntry.Requests, prefetchTrans)
	mshrEntry.ReadReq = readToBottom
	mshrEntry.Block = victim

	// Mark the block as being fetched
	victim.Tag = lineAddr
	victim.PID = pid
	victim.IsValid = true
	victim.IsLocked = true
	p.cache.directory.Visit(victim)

	// Add the transaction to the tracking list
	p.cache.postCoalesceTransactions = append(p.cache.postCoalesceTransactions, prefetchTrans)

	// Track the transaction
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

	p.logger.Printf("PREFETCH_TRANS ADDR:%x", lineAddr)
	return true
}

// RecordPrefetchHit should be called when a prefetched block is hit
func (p *StridePrefetcher) RecordPrefetchHit() {
	p.prefetchHits++
	p.logger.Printf("PREFETCH_HIT Total:%d", p.prefetchHits)
}

// RecordPrefetchComplete should be called when a prefetch request completes
func (p *StridePrefetcher) RecordPrefetchComplete() {
	p.completedPrefetcher++
	p.logger.Printf("PREFETCH_COMPLETE Total:%d", p.completedPrefetcher)
}

// Cleanup should be called when the prefetcher is no longer needed
func (p *StridePrefetcher) Cleanup() {
	// Log final statistics
	p.logger.Printf("FINAL_STATS PrefetchHits:%d PrefetchMisses:%d TotalPrefetches:%d SuccessfulPrefetches:%d CompletedPrefetches:%d",
		p.prefetchHits, p.prefetchMisses, p.totalPrefetches, p.successfulPrefetches, p.completedPrefetcher)

	p.logger.Printf("FINAL_METRICS Accuracy:%.2f%% SuccessRate:%.2f%% InCacheCount:%.0f",
		p.GetPrefetchAccuracy(), float64(p.successfulPrefetches)/float64(p.totalPrefetches)*100.0, p.GetInCache())

	// Close the log file
	if p.logFile != nil {
		p.logFile.Close()
	}
}

func (p *StridePrefetcher) GetPrefetchHits() uint64 {
	return p.prefetchHits
}

func (p *StridePrefetcher) GetPrefetchMisses() uint64 {
	return p.prefetchMisses
}

func (p *StridePrefetcher) GetTotalPrefetches() uint64 {
	return p.totalPrefetches
}

func (p *StridePrefetcher) GetSuccessfulPrefetches() uint64 {
	return p.successfulPrefetches
}

func (p *StridePrefetcher) GetCompletedPrefetches() uint64 {
	return p.completedPrefetcher
}

func (p *StridePrefetcher) GetPrefetchAccuracy() float64 {
	if p.successfulPrefetches == 0 {
		return 0.0
	}
	return float64(p.prefetchHits) / float64(p.successfulPrefetches) * 100.0
}

func (p *StridePrefetcher) GetInCache() float64 {
	return p.cache.directory.GetPrefetchedBlockCount()
}
