package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"os"
)

type metaOpration struct {
	from *net.UDPAddr
	m    *snet.Message
}

func (p *pd) onGetMeta(from *net.UDPAddr, m *snet.Message) {
	p.udp.SendTo(from, snet.MakeMessage(m.Context,
		&sproto.GetMetaResp{
			Version: p.pState.Meta.Version,
			Meta:    p.pState.MetaBytes,
		}))
}

func (p *pd) loadInitMeta() {
	if "" != p.config.InitMetaPath {
		f, err := os.Open(p.config.InitMetaPath)
		if nil == err {
			var b []byte
			for {
				data := make([]byte, 4096)
				count, err := f.Read(data)
				if count > 0 {
					b = append(b, data[:count]...)
				}

				if nil != err {
					break
				}
			}

			def, err := db.MakeDbDefFromJsonString(b)
			if nil != err {
				GetSugar().Panic(err)
			}

			dbc, err := sql.SqlOpen(p.config.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
			defer dbc.Close()

			if nil != err {
				GetSugar().Panic(err)
			}

			for _, v := range def.TableDefs {
				v.Version++
				tb, err := sql.GetTableScheme(dbc, p.config.DBType, fmt.Sprintf("%s_%d", v.Name, v.DbVersion))
				if nil != err {
					GetSugar().Panic(err)
				} else if nil == tb {
					//表不存在
					err = sql.CreateTables(dbc, p.config.DBType, v)
					if nil != err {
						GetSugar().Panic(err)
					} else {
						GetSugar().Infof("create table:%s_%d ok", v.Name, v.DbVersion)
					}
				} else if !v.Equal(*tb) {
					GetSugar().Panic(fmt.Sprintf("table:%s already in db but not match with meta", v.Name))
				} else {
					GetSugar().Infof("table:%s_%d is ok skip create", v.Name, v.DbVersion)
				}
			}

			def.Version++

			p.metaUpdateQueue.PushBack(&metaOpration{})

			p.issueProposal(&ProposalInitMeta{
				MetaDef: def,
				pd:      p,
			})
		}
	}
}

type ProposalInitMeta struct {
	proposalBase
	pd      *pd
	MetaDef *db.DbDef
}

func (p *ProposalInitMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalInitMeta, p)
}

func (p *ProposalInitMeta) OnError(err error) {
	p.pd.mainque.AppendHighestPriotiryItem(p.pd.onProposalUpdateMetaReply)
}

func (p *ProposalInitMeta) doapply(pd *pd) {
	pd.pState.Meta = *p.MetaDef
	pd.pState.MetaBytes, _ = p.MetaDef.ToJson()
	GetSugar().Infof("ProposalInitMeta apply version:%d", pd.pState.Meta.Version)
}

func (p *ProposalInitMeta) apply(pd *pd) {
	p.doapply(pd)
	pd.onProposalUpdateMetaReply()
}

func (p *ProposalInitMeta) replay(pd *pd) {
	p.doapply(pd)
}

type ProposalUpdateMeta struct {
	proposalBase
	pd     *pd
	TabDef *db.TableDef
}

func (p *ProposalUpdateMeta) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
	p.pd.mainque.AppendHighestPriotiryItem(p.pd.onProposalUpdateMetaReply)
}

func (p *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalUpdateMeta, p)
}

func (p *ProposalUpdateMeta) doApply(pd *pd) {
	def := &pd.pState.Meta
	i := 0
	for ; i < len(def.TableDefs); i++ {
		if def.TableDefs[i].Name == p.TabDef.Name {
			break
		}
	}

	def.Version++
	p.TabDef.Version++

	if i >= len(def.TableDefs) {
		def.TableDefs = append(def.TableDefs, p.TabDef)
		GetSugar().Infof("ProposalUpdateMeta add table def.version:%d tab.version:%d", def.Version, p.TabDef.Version)
	} else {
		def.TableDefs[i] = p.TabDef
		GetSugar().Infof("ProposalUpdateMeta add fields def.version:%d tab.version:%d", def.Version, p.TabDef.Version)
	}
	pd.pState.MetaBytes, _ = def.ToJson()
}

func (p *ProposalUpdateMeta) apply(pd *pd) {
	p.doApply(pd)
	if nil != p.reply {
		p.reply(nil)
	}
	p.pd.onProposalUpdateMetaReply()

	//notify all store leader
	for _, set := range pd.pState.deployment.sets {
		for _, node := range set.nodes {
			for storeID, store := range node.store {
				if store.isLead {
					addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.host, node.servicePort))
					pd.udp.SendTo(addr, snet.MakeMessage(0,
						&sproto.NotifyUpdateMeta{
							Store:   int32(storeID),
							Version: pd.pState.Meta.Version,
							Meta:    pd.pState.MetaBytes,
						}))
				}
			}
		}
	}

}

func (p *ProposalUpdateMeta) replay(pd *pd) {
	p.doApply(pd)
}

func (p *pd) onProposalUpdateMetaReply() {
	if p.metaUpdateQueue.Len() > 0 {
		p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
	}
	p.processMetaUpdate()
}

func (p *pd) onMetaAddTable(from *net.UDPAddr, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaAddTable)
	var tab *db.TableDef
	var err error

	def := p.pState.Meta.Clone()

	err = func() error {

		if msg.Version != def.Version {
			return errors.New("version mismatch")
		}

		for _, v := range def.TableDefs {
			if v.Name == msg.Name {
				return errors.New(fmt.Sprintf("table:%s already exists", v.Name))
			}
		}

		if len(msg.Fields) == 0 {
			return errors.New("len(Fields) == 0")
		}

		tab = &db.TableDef{Name: msg.Name, DbVersion: def.Version}
		for _, v := range msg.Fields {
			tab.Fields = append(tab.Fields, &db.FieldDef{
				Name:        v.Name,
				Type:        v.Type,
				StrCap:      int(v.Strcap),
				DefautValue: v.Default,
				TabVersion:  tab.Version,
			})
		}

		def.TableDefs = append(def.TableDefs, tab)
		if err := def.Check(); nil != err {
			return err
		}

		dbc, err := sql.SqlOpen(p.config.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
		defer dbc.Close()

		if nil != err {
			return err
		}

		dbtab, err := sql.GetTableScheme(dbc, p.config.DBType, fmt.Sprintf("%s_%d", tab.Name, tab.DbVersion))
		if nil != err {
			return err
		} else if nil == dbtab {
			//表不存在
			return sql.CreateTables(dbc, p.config.DBType, tab)
		} else if !tab.Equal(*dbtab) {
			return errors.New(fmt.Sprintf("table:%s already in db but not match with meta", tab.Name))
		} else {
			return nil
		}
	}()

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.MetaAddTableResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, &sproto.MetaAddTableResp{}),
			},
			TabDef: tab,
			pd:     p,
		})
		return true
	}

}

//向table添加fields
func (p *pd) onMetaAddFields(from *net.UDPAddr, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaAddFields)
	var tab *db.TableDef
	var err error
	def := p.pState.Meta.Clone()
	err = func() error {
		if msg.Version != def.Version {
			return errors.New("version mismatch")
		}

		if len(msg.Fields) == 0 {
			return errors.New("len(Fields) == 0")
		}

		for _, v := range def.TableDefs {
			if v.Name == msg.Table {
				tab = v.Clone()
			}
		}

		if nil == tab {
			return errors.New("table not found")
		}

		for _, v := range msg.Fields {
			tab.Fields = append(tab.Fields, &db.FieldDef{
				Name:        v.Name,
				Type:        v.Type,
				StrCap:      int(v.Strcap),
				DefautValue: v.Default,
				TabVersion:  tab.Version,
			})
		}

		if err := tab.Check(); nil != err {
			return err
		}

		dbc, err := sql.SqlOpen(p.config.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
		defer dbc.Close()

		if nil != err {
			return err
		}

		dbtab, err := sql.GetTableScheme(dbc, p.config.DBType, fmt.Sprintf("%s_%d", tab.Name, tab.DbVersion))
		if nil != err {
			return err
		} else if nil == dbtab {
			GetSugar().Errorf("table:%s in meta but not in db", tab.Name)
			//表不存在,不应该发生这种情况
			return sql.CreateTables(dbc, p.config.DBType, tab)
		} else if !tab.Equal(*dbtab) {
			tmp := tab.Clone()
			tmp.Fields = tmp.Fields[len(tmp.Fields)-len(msg.Fields):]
			return sql.AddFields(dbc, p.config.DBType, tmp)
		} else {
			return nil
		}
	}()

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.MetaAddFieldsResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, &sproto.MetaAddFieldsResp{}),
			},
			TabDef: tab,
			pd:     p,
		})
		return true
	}
}

func (p *pd) processMetaUpdate() {
	if p.metaUpdateQueue.Len() != 1 {
		return
	}
	for p.metaUpdateQueue.Len() > 0 {
		front := p.metaUpdateQueue.Front()
		op := front.Value.(*metaOpration)
		switch op.m.Msg.(type) {
		case *sproto.MetaAddTable:
			if !p.onMetaAddTable(op.from, op.m) {
				p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
			} else {
				return
			}
		case *sproto.MetaAddFields:
			if !p.onMetaAddFields(op.from, op.m) {
				p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
			} else {
				return
			}
		default:
			p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
		}
	}
}

func (p *pd) onUpdateMetaReq(from *net.UDPAddr, m *snet.Message) {
	p.metaUpdateQueue.PushBack(&metaOpration{
		from: from,
		m:    m,
	})
	p.processMetaUpdate()
}
