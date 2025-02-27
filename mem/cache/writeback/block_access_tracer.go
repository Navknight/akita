package writeback

import (
	"sync"
)

// CacheBlockAccessTracer is an interface that allows collecting statistics
// about cache block access patterns before eviction
type CacheBlockAccessTracer interface {
	// RecordBlockEviction records access counts from an evicted block
	RecordBlockEviction(readCount, writeCount int)

	// GetReadAccessCounts returns the histogram of read accesses
	GetReadAccessCounts() []int

	// GetWriteAccessCounts returns the histogram of write accesses
	GetWriteAccessCounts() []int

	// GetHistogramBinSize returns the bin size used for the histograms
	GetHistogramBinSize() int

	// GetTotalEvictions returns the total number of blocks evicted
	GetTotalEvictions() int
}

// BlockAccessTracer is an implementation of the CacheBlockAccessTracer interface
type BlockAccessTracer struct {
	sync.Mutex

	// Configuration
	HistogramBinSize   int
	MaxAccessesToTrack int

	// Statistics
	ReadAccessCounts  []int
	WriteAccessCounts []int
	TotalEvictions    int
}

// NewBlockAccessTracer creates a new tracer with the given bin size and max accesses
func NewBlockAccessTracer(binSize, maxAccesses int) *BlockAccessTracer {
	numBins := maxAccesses / binSize
	if maxAccesses%binSize != 0 {
		numBins++
	}

	return &BlockAccessTracer{
		HistogramBinSize:   binSize,
		MaxAccessesToTrack: maxAccesses,
		ReadAccessCounts:   make([]int, numBins),
		WriteAccessCounts:  make([]int, numBins),
	}
}

// RecordBlockEviction records access counts from an evicted block
func (t *BlockAccessTracer) RecordBlockEviction(readCount, writeCount int) {
	t.Lock()
	defer t.Unlock()

	t.TotalEvictions++

	// Record read accesses
	readBin := readCount / t.HistogramBinSize
	if readBin >= len(t.ReadAccessCounts) {
		readBin = len(t.ReadAccessCounts) - 1
	}
	t.ReadAccessCounts[readBin]++

	// Record write accesses
	writeBin := writeCount / t.HistogramBinSize
	if writeBin >= len(t.WriteAccessCounts) {
		writeBin = len(t.WriteAccessCounts) - 1
	}
	t.WriteAccessCounts[writeBin]++
}

// GetReadAccessCounts returns the histogram of read accesses
func (t *BlockAccessTracer) GetReadAccessCounts() []int {
	t.Lock()
	defer t.Unlock()

	// Return a copy to avoid concurrent modification issues
	result := make([]int, len(t.ReadAccessCounts))
	copy(result, t.ReadAccessCounts)
	return result
}

// GetWriteAccessCounts returns the histogram of write accesses
func (t *BlockAccessTracer) GetWriteAccessCounts() []int {
	t.Lock()
	defer t.Unlock()

	// Return a copy to avoid concurrent modification issues
	result := make([]int, len(t.WriteAccessCounts))
	copy(result, t.WriteAccessCounts)
	return result
}

// GetHistogramBinSize returns the bin size used for the histograms
func (t *BlockAccessTracer) GetHistogramBinSize() int {
	return t.HistogramBinSize
}

// GetTotalEvictions returns the total number of blocks evicted
func (t *BlockAccessTracer) GetTotalEvictions() int {
	t.Lock()
	defer t.Unlock()

	return t.TotalEvictions
}

// Reset resets all statistics
func (t *BlockAccessTracer) Reset() {
	t.Lock()
	defer t.Unlock()

	t.TotalEvictions = 0
	for i := range t.ReadAccessCounts {
		t.ReadAccessCounts[i] = 0
	}
	for i := range t.WriteAccessCounts {
		t.WriteAccessCounts[i] = 0
	}
}
