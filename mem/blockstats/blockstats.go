package blockstats

import "github.com/sarchlab/akita/v3/sim"

var HitBucketNames = []string{"0", "1", "2-3", "4-7", "8-15", "16+"}

type Entry struct {
	Live bool
	Key  uint64
	PID  uint64
	Hits uint64
	Fill sim.VTimeInSec
	Last sim.VTimeInSec
}

type Stats struct {
	Now func() sim.VTimeInSec

	Fills         uint64
	Bypassed      uint64
	Evicted       uint64
	Flushed       uint64
	DeadOnArrival uint64
	HitBuckets    [6]uint64
	LiveTime      sim.VTimeInSec
	DeadTime      sim.VTimeInSec
}

func (s *Stats) now() sim.VTimeInSec {
	if s.Now == nil {
		return 0
	}
	return s.Now()
}

func (s *Stats) Fill(e *Entry, key, pid uint64) {
	now := s.now()
	if e.Live {
		s.end(e, now, false)
	}

	*e = Entry{Live: true, Key: key, PID: pid, Fill: now, Last: now}
	s.Fills++
}

func (s *Stats) Hit(e *Entry) {
	e.Hits++
	e.Last = s.now()
}

func (s *Stats) Access(e *Entry, key, pid uint64) {
	if e.Live && e.Key == key && e.PID == pid {
		s.Hit(e)
		return
	}
	s.Fill(e, key, pid)
}

func (s *Stats) Flush(e *Entry) {
	if e.Live {
		s.end(e, s.now(), true)
	}
}

func (s *Stats) Bypass(e *Entry) {
	if e.Live {
		e.Live = false
		s.Fills--
		s.Bypassed++
	}
}

func (s *Stats) end(e *Entry, now sim.VTimeInSec, flushed bool) {
	e.Live = false

	if flushed {
		s.Flushed++
		return
	}

	s.Evicted++
	if e.Hits == 0 {
		s.DeadOnArrival++
	}
	s.HitBuckets[bucket(e.Hits)]++
	s.LiveTime += e.Last - e.Fill
	s.DeadTime += now - e.Last
}

func bucket(hits uint64) int {
	switch {
	case hits == 0:
		return 0
	case hits == 1:
		return 1
	case hits < 4:
		return 2
	case hits < 8:
		return 3
	case hits < 16:
		return 4
	default:
		return 5
	}
}
