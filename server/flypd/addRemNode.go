package flypd

import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
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
			RaftPort:    int(p.msg.RaftPort),
		},
		SetID: int(p.msg.SetID),
	}

	p.pd.pState.AddingNode[int(p.msg.NodeID)] = an
	if p.sendNotify {
		//向set内节点广播通告
		p.pd.sendNotifyAddNode(an)
		//启动定时器
		an.timer = time.AfterFunc(time.Second*3, func() {
			p.pd.mainque.AppendHighestPriotiryItem(an)
		})
	}

	if nil != p.reply {
		p.reply()
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
	an, ok := p.pd.pState.AddingNode[int(p.msg.NodeID)]

	if ok {

		find := false
		for i := 0; i < len(an.OkStores); i++ {
			if an.OkStores[i] == int(p.msg.Store) {
				find = true
				break
			}
		}

		if !find {
			an.OkStores = append(an.OkStores, int(p.msg.Store))
		}

		//GetSugar().Infof("ProposalNotifyAddNodeResp apply %d %d", an.OkStores, StorePerSet)

		if len(an.OkStores) == StorePerSet {
			if nil != an.timer {
				an.timer.Stop()
				an.timer = nil
			}

			//接收到所有store的应答
			delete(p.pd.pState.AddingNode, int(p.msg.NodeID))

			//添加到deployment中
			s := p.pd.deployment.sets[an.SetID]

			s.nodes[int(p.msg.NodeID)] = &kvnode{
				id:          int(an.NodeID),
				host:        an.Host,
				servicePort: int(an.ServicePort),
				raftPort:    int(an.RaftPort),
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

	p.pd.pState.RemovingNode[int(p.msg.NodeID)] = rn
	if p.sendNotify {
		//向set内节点广播通告
		p.pd.sendNotifyRemNode(rn)
		//启动定时器
		rn.timer = time.AfterFunc(time.Second*3, func() {
			p.pd.mainque.AppendHighestPriotiryItem(rn)
		})
	}

	if nil != p.reply {
		p.reply()
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
	rn, ok := p.pd.pState.RemovingNode[int(p.msg.NodeID)]
	if ok {
		find := false
		for i := 0; i < len(rn.OkStores); i++ {
			if rn.OkStores[i] == int(p.msg.Store) {
				find = true
				break
			}
		}

		if !find {
			rn.OkStores = append(rn.OkStores, int(p.msg.Store))
		}

		if len(rn.OkStores) == StorePerSet {
			if nil != rn.timer {
				rn.timer.Stop()
				rn.timer = nil
			}

			//接收到所有store的应答
			delete(p.pd.pState.RemovingNode, int(p.msg.NodeID))

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
	if len(rn.OkStores) < StorePerSet {
		s := p.deployment.sets[rn.SetID]
		notify := &sproto.NotifyRemNode{
			NodeID: int32(rn.NodeID),
		}

		rn.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答

		for _, v := range s.stores {
			find := false
			for i := 0; i < len(rn.OkStores); i++ {
				if rn.OkStores[i] == v.id {
					find = true
					break
				}
			}

			if !find {
				notify.Stores = append(notify.Stores, int32(v.id))
			}
		}

		for _, v := range s.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			p.udp.SendTo(addr, snet.MakeMessage(rn.context, notify))
		}
	}
}

func (p *pd) sendNotifyAddNode(an *AddingNode) {
	if len(an.OkStores) < StorePerSet {
		s := p.deployment.sets[an.SetID]
		notify := &sproto.NotifyAddNode{
			NodeID:   int32(an.NodeID),
			Host:     an.Host,
			RaftPort: int32(an.RaftPort),
		}

		an.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答

		for _, v := range s.stores {
			find := false
			for i := 0; i < len(an.OkStores); i++ {
				if an.OkStores[i] == v.id {
					find = true
					break
				}
			}
			if !find {
				notify.Stores = append(notify.Stores, int32(v.id))
			}
		}

		for _, v := range s.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			GetSugar().Infof("send notify add node to %s:%d", v.host, v.servicePort)
			p.udp.SendTo(addr, snet.MakeMessage(an.context, notify))
		}
	}
}
