// Package gmmu provides the implementation of the Graphics Memory Management Unit (GMMU).
// It includes structures and methods for handling memory translation, page migration,
// and other related operations within the virtual memory system.
package gmmu

import (
	"log"
	"reflect"

	"github.com/sarchlab/akita/v3/mem/vm"
	"github.com/sarchlab/akita/v3/sim"
	"github.com/sarchlab/akita/v3/tracing"
)

type transaction struct {
	req          *vm.TranslationReq
	invalidation *vm.PTEInvalidationReq
	page         vm.Page
	cycleLeft    int
}

// Comp is the default gmmu implementation. It is also an akita Component.
type Comp struct {
	sim.TickingComponent

	deviceID uint64

	topPort    sim.Port
	bottomPort sim.Port
	LowModule  sim.Port

	topSender    sim.BufferedSender
	bottomSender sim.BufferedSender

	pageTable           vm.PageTable
	log2PageSize        uint64
	latency             int
	maxRequestsInFlight int
	PWC                 *pwc
	AccessCounter       *vm.AccessCounter

	walkingTranslations []transaction
	remoteMemReqs       map[uint64]transaction

	toRemoveFromPTW        []int
	PageAccessedByDeviceID map[uint64][]uint64

	NumInvalidationWalks uint64
}

// Tick defines how the gmmu update state each cycle
func (gmmu *Comp) Tick(now sim.VTimeInSec) bool {
	madeProgress := false

	madeProgress = gmmu.topSender.Tick(now) || madeProgress
	madeProgress = gmmu.parseFromTop(now) || madeProgress
	madeProgress = gmmu.walkPageTable(now) || madeProgress
	madeProgress = gmmu.fetchFromBottom(now) || madeProgress
	madeProgress = gmmu.sendAccessCounterNotification(now) || madeProgress

	return madeProgress
}

func (gmmu *Comp) SetAccessCounter(c *vm.AccessCounter) {
	gmmu.AccessCounter = c
	c.OnPending = gmmu.TickLater
}

func (gmmu *Comp) sendAccessCounterNotification(now sim.VTimeInSec) bool {
	if gmmu.AccessCounter == nil || len(gmmu.AccessCounter.Pending) == 0 {
		return false
	}

	n := gmmu.AccessCounter.Pending[0]
	n.ID = sim.GetIDGenerator().Generate()
	n.Src = gmmu.bottomPort
	n.Dst = gmmu.LowModule
	n.SendTime = now

	if err := gmmu.bottomPort.Send(n); err != nil {
		return false
	}

	gmmu.AccessCounter.Pending = gmmu.AccessCounter.Pending[1:]

	return true
}

func (gmmu *Comp) parseFromTop(now sim.VTimeInSec) bool {
	if len(gmmu.walkingTranslations) >= gmmu.maxRequestsInFlight {
		return false
	}

	req := gmmu.topPort.Retrieve(now)
	if req == nil {
		return false
	}

	tracing.TraceReqReceive(req, gmmu)

	switch req := req.(type) {
	case *vm.TranslationReq:
		gmmu.startWalking(req)

		// fmt.Printf("%0.9f,%s,GMMUParseFromTop,%s\n",
		// 	float64(now), gmmu.topPort.Name(), req.TaskID)

	default:
		log.Panicf("gmmu canot handle request of type %s", reflect.TypeOf(req))
	}

	return true
}

func (gmmu *Comp) startWalking(req *vm.TranslationReq) {
	cached := gmmu.PWC.lookup(req.PID, req.VAddr>>gmmu.log2PageSize)

	translationInPipeline := transaction{
		req:       req,
		cycleLeft: (gmmu.PWC.numLevels - cached) * gmmu.latency,
	}

	gmmu.walkingTranslations = append(gmmu.walkingTranslations, translationInPipeline)
}

func (gmmu *Comp) walkPageTable(now sim.VTimeInSec) bool {
	madeProgress := false
	for i := 0; i < len(gmmu.walkingTranslations); i++ {
		if gmmu.walkingTranslations[i].cycleLeft > 0 {
			gmmu.walkingTranslations[i].cycleLeft--
			madeProgress = true
			continue
		}

		if gmmu.walkingTranslations[i].invalidation != nil {
			madeProgress = gmmu.finishInvalidationWalk(now, i) || madeProgress
			continue
		}

		req := gmmu.walkingTranslations[i].req
		gmmu.PWC.insert(req.PID, req.VAddr>>gmmu.log2PageSize)

		page, _ := gmmu.pageTable.Find(req.PID, req.VAddr)

		if page.DeviceID == gmmu.deviceID && !page.IsMigrating {
			madeProgress = gmmu.finalizePageWalk(now, i) || madeProgress
		} else {
			madeProgress = gmmu.processRemoteMemReq(now, i) || madeProgress
		}
	}

	tmp := gmmu.walkingTranslations[:0]
	for i := 0; i < len(gmmu.walkingTranslations); i++ {
		if !gmmu.toRemove(i) {
			tmp = append(tmp, gmmu.walkingTranslations[i])
		}
	}
	gmmu.walkingTranslations = tmp
	gmmu.toRemoveFromPTW = nil

	return madeProgress
}

func (gmmu *Comp) processRemoteMemReq(now sim.VTimeInSec, walkingIndex int) bool {
	// if !gmmu.bottomSender.CanSend(1) {
	// 	return false
	// }

	walking := gmmu.walkingTranslations[walkingIndex].req

	req := vm.TranslationReqBuilder{}.
		WithSendTime(now).
		WithSrc(gmmu.bottomPort).
		WithDst(gmmu.LowModule).
		WithPID(walking.PID).
		WithVAddr(walking.VAddr).
		WithDeviceID(walking.DeviceID).
		Build()

	err := gmmu.bottomPort.Send(req)

	if err != nil {
		return false
	}
	gmmu.remoteMemReqs[walking.VAddr] = gmmu.walkingTranslations[walkingIndex]

	gmmu.toRemoveFromPTW = append(gmmu.toRemoveFromPTW, walkingIndex)

	return true
}

func (gmmu *Comp) finalizePageWalk(
	now sim.VTimeInSec,
	walkingIndex int,
) bool {
	req := gmmu.walkingTranslations[walkingIndex].req
	page, _ := gmmu.pageTable.Find(req.PID, req.VAddr)
	gmmu.walkingTranslations[walkingIndex].page = page

	return gmmu.doPageWalkHit(now, walkingIndex)
}

func (gmmu *Comp) doPageWalkHit(
	now sim.VTimeInSec,
	walkingIndex int,
) bool {
	if !gmmu.topSender.CanSend(1) {
		return false
	}
	walking := gmmu.walkingTranslations[walkingIndex]

	rsp := vm.TranslationRspBuilder{}.
		WithSendTime(now).
		WithSrc(gmmu.topPort).
		WithDst(walking.req.Src).
		WithRspTo(walking.req.ID).
		WithPage(walking.page).
		Build()

	gmmu.topSender.Send(rsp)

	gmmu.toRemoveFromPTW = append(gmmu.toRemoveFromPTW, walkingIndex)

	tracing.TraceReqComplete(walking.req, gmmu)

	return true
}

func (gmmu *Comp) toRemove(index int) bool {
	for i := 0; i < len(gmmu.toRemoveFromPTW); i++ {
		remove := gmmu.toRemoveFromPTW[i]
		if remove == index {
			return true
		}
	}
	return false
}

func (gmmu *Comp) fetchFromBottom(now sim.VTimeInSec) bool {
	msg := gmmu.bottomPort.Peek()
	if msg == nil {
		return false
	}

	switch msg := msg.(type) {
	case *vm.TranslationRsp:
		if !gmmu.topSender.CanSend(1) {
			return false
		}

		gmmu.bottomPort.Retrieve(now)
		tracing.TraceReqReceive(msg, gmmu)

		return gmmu.handleTranslationRsp(now, msg)
	case *vm.PTEInvalidationReq:
		if len(gmmu.walkingTranslations) >= gmmu.maxRequestsInFlight {
			return false
		}

		gmmu.bottomPort.Retrieve(now)
		gmmu.startInvalidationWalk(msg)

		return true
	default:
		log.Panicf("gmmu canot handle request of type %s", reflect.TypeOf(msg))
	}

	return true
}

func (gmmu *Comp) startInvalidationWalk(inv *vm.PTEInvalidationReq) {
	cached := gmmu.PWC.lookup(inv.PID, inv.VAddr>>gmmu.log2PageSize)

	gmmu.walkingTranslations = append(gmmu.walkingTranslations, transaction{
		invalidation: inv,
		cycleLeft:    (gmmu.PWC.numLevels - cached) * gmmu.latency,
	})
}

func (gmmu *Comp) finishInvalidationWalk(now sim.VTimeInSec, i int) bool {
	inv := gmmu.walkingTranslations[i].invalidation
	gmmu.PWC.insert(inv.PID, inv.VAddr>>gmmu.log2PageSize)

	rsp := vm.NewPTEInvalidationRsp(now, gmmu.bottomPort, inv.Src, inv.ID)
	if err := gmmu.bottomPort.Send(rsp); err != nil {
		return false
	}

	gmmu.NumInvalidationWalks++
	gmmu.toRemoveFromPTW = append(gmmu.toRemoveFromPTW, i)

	return true
}

func (gmmu *Comp) handleTranslationRsp(now sim.VTimeInSec, rsponse *vm.TranslationRsp) bool {
	t, ok := gmmu.remoteMemReqs[rsponse.Page.VAddr]
	if !ok {
		return true
	}
	rsp := vm.TranslationRspBuilder{}.
		WithSendTime(now).
		WithSrc(gmmu.topPort).
		WithDst(t.req.Src).
		WithRspTo(t.req.ID).
		WithPage(rsponse.Page).
		Build()

	gmmu.topSender.Send(rsp)

	delete(gmmu.remoteMemReqs, rsponse.Page.VAddr)
	return true
}
