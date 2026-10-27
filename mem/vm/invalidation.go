package vm

import "github.com/sarchlab/akita/v3/sim"

type PTEInvalidationReq struct {
	sim.MsgMeta
	PID   PID
	VAddr uint64
}

func (r *PTEInvalidationReq) Meta() *sim.MsgMeta {
	return &r.MsgMeta
}

func NewPTEInvalidationReq(
	now sim.VTimeInSec,
	src, dst sim.Port,
	pid PID,
	vAddr uint64,
) *PTEInvalidationReq {
	r := &PTEInvalidationReq{PID: pid, VAddr: vAddr}
	r.ID = sim.GetIDGenerator().Generate()
	r.Src = src
	r.Dst = dst
	r.SendTime = now
	return r
}

type PTEInvalidationRsp struct {
	sim.MsgMeta
	RespondTo string
}

func (r *PTEInvalidationRsp) Meta() *sim.MsgMeta {
	return &r.MsgMeta
}

func (r *PTEInvalidationRsp) GetRspTo() string {
	return r.RespondTo
}

func NewPTEInvalidationRsp(
	now sim.VTimeInSec,
	src, dst sim.Port,
	rspTo string,
) *PTEInvalidationRsp {
	r := &PTEInvalidationRsp{RespondTo: rspTo}
	r.ID = sim.GetIDGenerator().Generate()
	r.Src = src
	r.Dst = dst
	r.SendTime = now
	return r
}
