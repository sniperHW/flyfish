package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	fproto "github.com/sniperHW/flyfish/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"strings"
	"time"
)

type Meta struct {
	Version   int64
	MetaBytes []byte
	MetaDef   *db.DbDef
}

func (m Meta) isEqual(b []byte) bool {
	if len(m.MetaBytes) != len(b) {
		return false
	}
	for i := 0; i < len(m.MetaBytes); i++ {
		if m.MetaBytes[i] != b[i] {
			return false
		}
	}
	return true
}

func checkDbDef(def *db.DbDef) error {
	tables := map[string]bool{}

	for _, v := range def.TableDefs {
		if _, ok := tables[v.Name]; ok {
			return errors.New("duplicate table")
		} else {
			tables[v.Name] = true
		}

		fields := map[string]bool{}
		for _, vv := range v.Fields {
			if _, ok := fields[vv.Name]; ok {
				return fmt.Errorf("table:%s duplicate field:%s", v.Name, vv.Name)
			} else {
				fields[vv.Name] = true
			}

			if strings.HasPrefix(vv.Name, "__") {
				return fmt.Errorf("table:%s invaild field:%s", v.Name, vv.Name)
			}

			ftype := db.GetTypeByStr(vv.Type)

			if ftype == fproto.ValueType_invaild {
				return fmt.Errorf("table:%s field:%s invaild type", v.Name, vv.Name)
			}

			defaultValue := db.GetDefaultValue(ftype, vv.DefautValue)

			if nil == defaultValue {
				return fmt.Errorf("table:%s field:%s invaild default value", v.Name, vv.Name)
			}
		}
	}
	return nil
}

func (p *pd) checkMeta(meta []byte) (*db.DbDef, error) {
	def, err := db.CreateDbDefFromJsonString(meta)
	if nil != err {
		return nil, err
	}

	return def, checkDbDef(def)
}

func (p *pd) onGetMeta(from *net.UDPAddr, m *snet.Message) {
	p.udp.SendTo(from, snet.MakeMessage(m.Context,
		&sproto.GetMetaResp{
			Version: p.pState.Meta.Version,
			Meta:    p.pState.Meta.MetaBytes,
		}))
}

type ProposalSetMeta struct {
	proposalBase
	MetaBytes []byte
	metaDef   *db.DbDef
}

func (p *ProposalSetMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSetMeta, p)
}

func (p *ProposalSetMeta) apply(pd *pd) {
	pd.pState.Meta.Version++
	pd.pState.Meta.MetaDef = p.metaDef
	pd.pState.Meta.MetaBytes = p.MetaBytes

	if nil != p.reply {
		p.reply(nil)
	}
}

func (p *ProposalSetMeta) replay(pd *pd) {
	p.metaDef, _ = db.CreateDbDefFromJsonString(p.MetaBytes)
	p.apply(pd)
}

/*
 *  直接设置meta,可以对表或字段执行变更，不应当在flykv运行阶段执行
 */

func (p *pd) onSetMeta(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMeta)
	resp := &sproto.SetMetaResp{}

	if p.pState.Meta.isEqual(msg.Meta) {
		resp.Ok = true
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	def, err := p.checkMeta(msg.Meta)
	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	p.issueProposal(&ProposalSetMeta{
		MetaBytes: msg.Meta,
		metaDef:   def,
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(from, m, resp),
		},
	})
}

type MetaTransactionStore struct {
	StoreID int
	Ok      bool
}

type MetaTransaction struct {
	MetaDef *db.DbDef
	Store   []MetaTransactionStore
	Version int64
	timer   *time.Timer
}

func (m *MetaTransaction) notifyStore(p *pd) {
	if nil == p.pState.MetaTransaction {
		return
	}

	c := 0
	for _, v := range m.Store {
		if !v.Ok {
			c++
			s := p.pState.deployment.getStoreByID(v.StoreID)
			for _, vv := range s.set.nodes {
				addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", vv.host, vv.servicePort))
				p.udp.SendTo(addr, snet.MakeMessage(0,
					&sproto.NotifyUpdateMeta{
						Store:   int32(v.StoreID),
						Version: int64(p.pState.Meta.Version),
						Meta:    p.pState.Meta.MetaBytes,
					}))
			}
		}
	}

	if c > 0 {
		m.timer = time.AfterFunc(time.Second, func() {
			p.mainque.AppendHighestPriotiryItem(m)
		})
	}
}

type ProposalUpdateMeta struct {
	proposalBase
	Tran *MetaTransaction
}

func (p *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalUpdateMeta, p)
}

func (p *ProposalUpdateMeta) apply(pd *pd) {
	var err error
	if nil != pd.pState.MetaTransaction {
		err = errors.New("wait for previous meta transaction finish")
	} else {
		pd.pState.Meta.Version = p.Tran.Version
		pd.pState.Meta.MetaDef = p.Tran.MetaDef
		pd.pState.Meta.MetaBytes, _ = db.DbDefToJsonString(pd.pState.Meta.MetaDef)
		if len(p.Tran.Store) > 0 {
			pd.pState.MetaTransaction = p.Tran
			pd.pState.MetaTransaction.notifyStore(pd)
		}
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *ProposalUpdateMeta) replay(pd *pd) {
	p.apply(pd)
}

//运行期间更新meta，只允许添加
func (p *pd) onUpdateMeta(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.UpdateMeta)
	resp := &sproto.UpdateMetaResp{}

	if nil != p.pState.MetaTransaction {
		resp.Ok = false
		resp.Reason = "wait for previous meta transaction finish"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	t := &MetaTransaction{
		MetaDef: p.pState.Meta.MetaDef.Clone(),
		Version: p.pState.Meta.Version + 1,
	}

	for _, v := range msg.Updates {
		tb := t.MetaDef.GetTableDef(v.Name)
		if nil == tb {
			tb = &db.TableDef{
				Name: v.Name,
			}
			t.MetaDef.TableDefs = append(t.MetaDef.TableDefs, tb)
		}

		for _, vv := range v.Fields {
			tb.Fields = append(tb.Fields, &db.FieldDef{
				Name:        vv.Name,
				Type:        vv.Type,
				DefautValue: vv.Default,
			})
		}
	}

	if err := checkDbDef(t.MetaDef); nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.UpdateMetaResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return
	}

	if nil != p.pState.deployment {
		for _, s := range p.pState.deployment.sets {
			for kk, _ := range s.stores {
				t.Store = append(t.Store, MetaTransactionStore{
					StoreID: kk,
					Ok:      false,
				})
			}
		}
	}

	p.issueProposal(&ProposalUpdateMeta{
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(from, m, resp),
		},
		Tran: t,
	})
}

type ProposalStoreUpdateMetaOk struct {
	proposalBase
	Store int
}

func (p *ProposalStoreUpdateMetaOk) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalStoreUpdateMetaOk, p)
}

func (p *ProposalStoreUpdateMetaOk) apply(pd *pd) {
	t := pd.pState.MetaTransaction
	c := 0
	for k, v := range t.Store {
		if v.StoreID == p.Store {
			t.Store[k].Ok = true
		} else if !v.Ok {
			c++
		}
	}
	if c == 0 {
		//所有store均已应答，事务结束
		pd.pState.MetaTransaction = nil
	}
}

func (p *ProposalStoreUpdateMetaOk) replay(pd *pd) {
	p.apply(pd)
}

func (p *pd) onStoreUpdateMetaOk(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.StoreUpdateMetaOk)
	t := p.pState.MetaTransaction
	if t != nil && t.Version == msg.Version {
		store := -1
		for _, v := range t.Store {
			if v.StoreID == int(msg.Store) {
				if !v.Ok {
					store = v.StoreID
				}
				break
			}
		}

		if store != -1 {
			p.issueProposal(&ProposalStoreUpdateMetaOk{
				Store: store,
			})
		}
	}
}
