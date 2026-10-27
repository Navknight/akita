package vm

import "github.com/sarchlab/akita/v3/sim"

type AccessCounterNotification struct {
	sim.MsgMeta
	PID      PID
	VAddr    uint64
	DeviceID uint64
}

func (m *AccessCounterNotification) Meta() *sim.MsgMeta {
	return &m.MsgMeta
}

type accessCounterKey struct {
	pid   PID
	group uint64
}

type AccessCounter struct {
	threshold     uint64
	log2GroupSize uint64
	counts        map[accessCounterKey]uint64

	Pending          []*AccessCounterNotification
	NumNotifications uint64
	OnPending        func(now sim.VTimeInSec)
}

func NewAccessCounter(threshold, log2GroupSize uint64) *AccessCounter {
	return &AccessCounter{
		threshold:     threshold,
		log2GroupSize: log2GroupSize,
		counts:        make(map[accessCounterKey]uint64),
	}
}

func (c *AccessCounter) Count(
	now sim.VTimeInSec,
	pid PID,
	pageVAddr, deviceID uint64,
) {
	k := accessCounterKey{pid: pid, group: pageVAddr >> c.log2GroupSize}
	c.counts[k]++
	if c.counts[k] < c.threshold {
		return
	}

	delete(c.counts, k)
	c.NumNotifications++
	c.Pending = append(c.Pending, &AccessCounterNotification{
		PID:      pid,
		VAddr:    pageVAddr,
		DeviceID: deviceID,
	})

	if c.OnPending != nil {
		c.OnPending(now)
	}
}
