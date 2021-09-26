package flypd

import (
	"encoding/json"
	//"errors"
	"fmt"
	//"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	//"github.com/sniperHW/flyfish/pkg/timer"
	sproto "github.com/sniperHW/flyfish/server/proto"
	//"github.com/sniperHW/flyfish/server/slot"
	"net"
	"time"
)

type ProposalAddNode struct {
	*proposalBase
	msg        *sproto.AddNode
	sendNotify bool
}

func (p *ProposalAddNode) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddNode))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalAddNode) apply() {
	an := &AddingNode{
		KvNodeJson: KvNodeJson{
			NodeID:      int(p.msg.NodeID),
			Host:        p.msg.Host,
			ServicePort: int(p.msg.ServicePort),
			InterPort:   int(p.msg.InterPort),
		},
		SetID: int(p.msg.SetID),
	}

	p.pd.addingNode[int(p.msg.NodeID)] = an
	if p.sendNotify {
		//向set内节点广播通告
		p.pd.sendNotifyAddNode(an)
		//启动定时器
		an.timer = time.AfterFunc(time.Second*3, func() {
			p.pd.mainque.AppendHighestPriotiryItem(an)
		})
	}
}

func (p *pd) replayAddNode(reader *buffer.BufferReader) error {
	var msg sproto.AddNode
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pa := &ProposalAddNode{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pa.apply()
	return nil
}

type ProposalNotifyAddNodeResp struct {
	*proposalBase
	msg *sproto.NotifyAddNodeResp
}

func (p *ProposalNotifyAddNodeResp) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalNotifyAddNodeResp))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalNotifyAddNodeResp) apply() {
	an, ok := p.pd.addingNode[int(p.msg.NodeID)]
	if ok {
		var i int

		for i := 0; i < len(an.OkStores); i++ {
			if an.OkStores[i] == int(p.msg.Store) {
				break
			}
		}

		if i == len(an.OkStores) {
			an.OkStores = append(an.OkStores, int(p.msg.Store))
		}

		if len(an.OkStores) == StorePerSet {
			if nil != an.timer {
				an.timer.Stop()
				an.timer = nil
			}

			//接收到所有store的应答
			delete(p.pd.addingNode, int(p.msg.NodeID))

			//添加到deployment中
			s := p.pd.deployment.sets[an.SetID]

			s.nodes[int(p.msg.NodeID)] = &kvnode{
				id:          int(an.NodeID),
				host:        an.Host,
				servicePort: int(an.ServicePort),
				interPort:   int(an.InterPort),
				set:         s,
			}
		}
	}
}

func (p *pd) replayNotifyAddNodeResp(reader *buffer.BufferReader) error {
	var msg sproto.NotifyAddNodeResp
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}
	pp := &ProposalNotifyAddNodeResp{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pp.apply()
	return nil
}

type ProposalRemNode struct {
	*proposalBase
	sendNotify bool
	msg        *sproto.RemNode
}

func (p *ProposalRemNode) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalRemNode))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalRemNode) apply() {
	rn := &RemovingNode{
		NodeID: int(p.msg.NodeID),
		SetID:  int(p.msg.SetID),
	}

	p.pd.removingNode[int(p.msg.NodeID)] = rn
	if p.sendNotify {
		//向set内节点广播通告
		p.pd.sendNotifyRemNode(rn)
		//启动定时器
		rn.timer = time.AfterFunc(time.Second*3, func() {
			p.pd.mainque.AppendHighestPriotiryItem(rn)
		})
	}
}

func (p *pd) replayRemNode(reader *buffer.BufferReader) error {
	var msg sproto.RemNode
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pr := &ProposalRemNode{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pr.apply()
	return nil
}

type ProposalNotifyRemNodeResp struct {
	*proposalBase
	msg *sproto.NotifyRemNodeResp
}

func (p *ProposalNotifyRemNodeResp) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalNotifyRemNodeResp))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalNotifyRemNodeResp) apply() {
	rn, ok := p.pd.removingNode[int(p.msg.NodeID)]
	if ok {
		var i int

		for i := 0; i < len(rn.OkStores); i++ {
			if rn.OkStores[i] == int(p.msg.Store) {
				break
			}
		}

		if i == len(rn.OkStores) {
			rn.OkStores = append(rn.OkStores, int(p.msg.Store))
		}

		if len(rn.OkStores) == StorePerSet {
			if nil != rn.timer {
				rn.timer.Stop()
				rn.timer = nil
			}

			//接收到所有store的应答
			delete(p.pd.removingNode, int(p.msg.NodeID))

			//从deployment中移除
			s := p.pd.deployment.sets[rn.SetID]
			delete(s.nodes, rn.NodeID)
		}
	}
}

func (p *pd) replayNotifyRemNodeResp(reader *buffer.BufferReader) error {
	var msg sproto.NotifyRemNodeResp
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}
	pp := &ProposalNotifyRemNodeResp{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pp.apply()
	return nil
}

func (p *pd) sendNotifyRemNode(rn *RemovingNode) {
	s := p.deployment.sets[rn.SetID]
	notify := &sproto.NotifyRemNode{
		NodeID: int32(rn.NodeID),
	}

	for _, v := range s.stores {
		var i int
		for i = 0; i < len(rn.OkStores); i++ {
			if rn.OkStores[i] == v.id {
				break
			}
		}
		if i == len(rn.OkStores) {
			notify.Stores = append(notify.Stores, int32(v.id))
		}
	}

	for _, v := range s.nodes {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.interPort))
		p.udp.SendTo(addr, notify)
	}
}

func (p *pd) sendNotifyAddNode(an *AddingNode) {
	s := p.deployment.sets[an.SetID]
	notify := &sproto.NotifyAddNode{
		NodeID:    int32(an.NodeID),
		Host:      an.Host,
		InterPort: int32(an.InterPort),
	}

	for _, v := range s.stores {
		var i int
		for i = 0; i < len(an.OkStores); i++ {
			if an.OkStores[i] == v.id {
				break
			}
		}
		if i == len(an.OkStores) {
			notify.Stores = append(notify.Stores, int32(v.id))
		}
	}

	for _, v := range s.nodes {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.interPort))
		p.udp.SendTo(addr, notify)
	}
}
