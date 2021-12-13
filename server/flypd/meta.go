package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	fproto "github.com/sniperHW/flyfish/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"strings"
)

type Meta struct {
	Version   int64
	MetaBytes []byte
	metaDef   *db.DbDef
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
			Version: p.meta.Version,
			Meta:    p.meta.MetaBytes,
		}))
}

type ProposalSetMeta struct {
	*proposalBase
	metaBytes []byte
	metaDef   *db.DbDef
}

func (p *ProposalSetMeta) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalSetMeta))
	b = buffer.AppendUint32(b, uint32(len(p.metaBytes)))
	return buffer.AppendBytes(b, p.metaBytes)
}

func (p *ProposalSetMeta) apply() {
	p.pd.meta.Version++
	p.pd.meta.metaDef = p.metaDef
	p.pd.meta.MetaBytes = p.metaBytes
	if nil != p.reply {
		p.reply()
	}
}

func (p *pd) replaySetMeta(reader *buffer.BufferReader) error {

	l, err := reader.CheckGetUint32()
	if nil != err {
		return err
	}

	b, err := reader.CheckGetBytes(int(l))
	if nil != err {
		return err
	}

	def, err := db.CreateDbDefFromJsonString(b)
	if nil != err {
		return err
	}

	p.meta.Version++
	p.meta.metaDef = def
	p.meta.MetaBytes = b

	return nil
}

/*
 *  直接设置meta,可以对表或字段执行变更，不应当在flykv运行阶段执行
 */

func (p *pd) onSetMeta(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMeta)

	if p.meta.isEqual(msg.Meta) {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMetaResp{
				Ok: true,
			}))
		return
	}

	def, err := p.checkMeta(msg.Meta)
	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMetaResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return
	}

	err = p.issueProposal(&ProposalSetMeta{
		metaBytes: msg.Meta,
		metaDef:   def,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.SetMetaResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.SetMetaResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMetaResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}
}

type MetaTransactionStore struct {
	StoreID int
	Ok      bool
}

type MetaTransaction struct {
	MetaDef    *db.DbDef
	Store      []MetaTransactionStore
	Prepareing bool
}

func (m *MetaTransaction) notifyStore(p *pd) {

}

type ProposalUpdateMeta struct {
	*proposalBase
}

func (p *ProposalUpdateMeta) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalUpdateMeta))
	j, _ := json.Marshal(p.pd.metaTransaction)
	b = buffer.AppendUint32(b, uint32(len(j)))
	return buffer.AppendBytes(b, j)
}

func (p *ProposalUpdateMeta) apply() {
	p.pd.meta.Version++
	p.pd.meta.metaDef = p.pd.metaTransaction.MetaDef
	p.pd.meta.MetaBytes, _ = db.DbDefToJsonString(p.pd.meta.metaDef)
	if len(p.pd.metaTransaction.Store) == 0 {
		//无需通知任何store,事务结束
		p.pd.metaTransaction = nil
	} else {
		p.pd.metaTransaction.Prepareing = false
		p.pd.metaTransaction.notifyStore(p.pd)
	}
	if nil != p.reply {
		p.reply()
	}
}

func (p *pd) replayUpdateMeta(reader *buffer.BufferReader) error {

	l, err := reader.CheckGetUint32()
	if nil != err {
		return err
	}

	b, err := reader.CheckGetBytes(int(l))
	if nil != err {
		return err
	}

	t := &MetaTransaction{}
	err = json.Unmarshal(b, t)
	if nil != err {
		return err
	}

	if nil != p.metaTransaction {
		return errors.New("nil != p.pd.metaTransaction")
	}

	p.meta.Version++
	p.meta.metaDef = t.MetaDef
	p.meta.MetaBytes, _ = db.DbDefToJsonString(p.meta.metaDef)

	if len(t.Store) > 0 {
		p.metaTransaction = t
	}

	return nil
}

//运行期间更新meta，只允许添加
func (p *pd) onUpdateMeta(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.UpdateMeta)
	if nil != p.metaTransaction {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.UpdateMetaResp{
				Ok:     false,
				Reason: "wait for previous meta transaction finish",
			}))
		return
	}

	t := MetaTransaction{
		Prepareing: true,
		MetaDef:    p.meta.metaDef.Clone(),
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

	if nil != p.deployment {
		for _, s := range p.deployment.sets {
			for kk, _ := range s.stores {
				t.Store = append(t.Store, MetaTransactionStore{
					StoreID: kk,
					Ok:      false,
				})
			}
		}
	}

	p.metaTransaction = &t

	err := p.issueProposal(&ProposalUpdateMeta{
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.UpdateMetaResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.UpdateMetaResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.UpdateMetaResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}
