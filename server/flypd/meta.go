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
	replyer replyer
	m       *snet.Message
}

func (p *pd) onGetMeta(replyer replyer, m *snet.Message) {
	replyer.reply(snet.MakeMessage(m.Context,
		&sproto.GetMetaResp{
			Version: p.pState.Meta.Version,
			Meta:    p.pState.MetaBytes,
		}))
}

func (p *pd) loadInitMeta() {
	GetSugar().Infof("loadInitMeta:%s", p.config.InitMetaPath)
	dbc, err := sql.SqlOpen(p.config.DBConfig.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
	defer dbc.Close()
	if nil != err {
		GetSugar().Panic(err)
	}

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

			for _, t := range def.TableDefs {
				tb, err := sql.GetTableScheme(dbc, p.config.DBConfig.DBType, fmt.Sprintf("%s_%d", t.Name, t.DbVersion))
				if nil != err {
					GetSugar().Panic(err)
				} else if nil == tb {
					t.Version++
					//表不存在
					err = sql.CreateTables(dbc, p.config.DBConfig.DBType, t)
					if nil != err {
						GetSugar().Panic(err)
					} else {
						GetSugar().Infof("create table:%s_%d ok", t.Name, t.DbVersion)
					}
				} else {
					//表在db中已经存在，用db中的信息修正meta
					t.Version = tb.Version + 1
					//记录字段的最大版本
					fields := map[string]*db.FieldDef{}
					for _, v := range tb.Fields {
						f := fields[v.Name]
						if nil == f || f.TabVersion <= v.TabVersion {
							fields[v.Name] = v
						}
					}

					for _, v := range t.Fields {
						f, ok := fields[v.Name]
						if !ok {
							GetSugar().Panicf("table:%s already in db but not match with meta,field:%s not found in db", t.Name, v.Name)
						}

						if f.Type != v.Type {
							GetSugar().Panicf("table:%s already in db but not match with meta,field:%s type mismatch with db", t.Name, v.Name)
						}

						if f.GetDefaultValueStr() != v.GetDefaultValueStr() {
							GetSugar().Panicf("table:%s already in db but not match with meta,field:%s DefaultValue mismatch with db db.v:%v meta.v:%v", t.Name, v.Name, f.GetDefaultValueStr(), v.GetDefaultValueStr())
						}

						v.TabVersion = f.TabVersion
					}
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

func (p *ProposalInitMeta) apply(pd *pd) {
	pd.pState.Meta = *p.MetaDef
	pd.pState.MetaBytes, _ = p.MetaDef.ToJson()
	GetSugar().Infof("ProposalInitMeta apply version:%d", pd.pState.Meta.Version)

	//for _, v := range pd.pState.Meta.TableDefs {
	//	GetSugar().Infof("ProposalInitMeta apply tab:%s version:%d", v.Name, v.Version)
	//}

	if pd.isLeader() {
		pd.onProposalUpdateMetaReply()
	}

}

type MetaUpdateType int

const (
	MetaAddTable     = MetaUpdateType(1)
	MetaAddFields    = MetaUpdateType(2)
	MetaRemoveTable  = MetaUpdateType(3)
	MetaRemoveFields = MetaUpdateType(4)
)

type ProposalUpdateMeta struct {
	proposalBase
	pd         *pd
	TabDef     *db.TableDef
	UpdateType MetaUpdateType
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

func (p *ProposalUpdateMeta) apply(pd *pd) {
	def := &pd.pState.Meta
	i := 0

	if p.UpdateType != MetaAddTable {
		for ; i < len(def.TableDefs); i++ {
			if def.TableDefs[i].Name == p.TabDef.Name {
				break
			}
		}
	}

	def.Version++
	if p.UpdateType == MetaRemoveTable {
		last := len(def.TableDefs) - 1
		def.TableDefs[i], def.TableDefs[last] = def.TableDefs[last], def.TableDefs[i]
		def.TableDefs = def.TableDefs[:last]
		GetSugar().Infof("ProposalUpdateMeta remove table def.version:%d", def.Version)
	} else {
		p.TabDef.Version++
		if p.UpdateType == MetaAddTable {
			def.TableDefs = append(def.TableDefs, p.TabDef)
			GetSugar().Infof("ProposalUpdateMeta add table def.version:%d tab.version:%d", def.Version, p.TabDef.Version)
		} else {
			def.TableDefs[i] = p.TabDef
			if p.UpdateType == MetaAddFields {
				GetSugar().Infof("ProposalUpdateMeta add fields def.version:%d tab.version:%d", def.Version, p.TabDef.Version)
			} else {
				GetSugar().Infof("ProposalUpdateMeta remove fields def.version:%d tab.version:%d", def.Version, p.TabDef.Version)
			}
		}
	}

	pd.pState.MetaBytes, _ = def.ToJson()

	if pd.isLeader() {

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
}

func (p *pd) onProposalUpdateMetaReply() {
	if p.metaUpdateQueue.Len() > 0 {
		p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
	}
	p.processMetaUpdate()
}

func (p *pd) onMetaAddTable(replyer replyer, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaAddTable)
	var tab *db.TableDef
	var err error

	def := p.pState.Meta.Clone()

	err = func() error {

		if msg.Version != def.Version {
			return errors.New("version mismatch")
		}

		if msg.Name == "" {
			return errors.New("table name is empty")
		}

		for _, v := range def.TableDefs {
			if v.Name == msg.Name {
				return fmt.Errorf("table:%s already exists", v.Name)
			}
		}

		if len(msg.Fields) == 0 {
			return errors.New("len(Fields) == 0")
		}

		tab = &db.TableDef{Name: msg.Name, DbVersion: def.Version}
		for _, v := range msg.Fields {
			tab.Fields = append(tab.Fields, &db.FieldDef{
				Name:         v.Name,
				Type:         v.Type,
				DefaultValue: v.Default,
				TabVersion:   tab.Version,
			})
		}

		def.TableDefs = append(def.TableDefs, tab)
		if err := def.Check(); nil != err {
			return err
		}

		dbc, err := sql.SqlOpen(p.config.DBConfig.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
		defer dbc.Close()

		if nil != err {
			return err
		}

		dbtab, err := sql.GetTableScheme(dbc, p.config.DBConfig.DBType, fmt.Sprintf("%s_%d", tab.Name, tab.DbVersion))
		if nil != err {
			return err
		} else if nil == dbtab {
			//表不存在
			return sql.CreateTables(dbc, p.config.DBConfig.DBType, tab)
		} else {
			return fmt.Errorf("table:%s_%d already in db but not match with meta", tab.Name, tab.DbVersion)
		}
	}()

	if nil != err {
		replyer.reply(snet.MakeMessage(m.Context,
			&sproto.MetaAddTableResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, &sproto.MetaAddTableResp{}),
			},
			TabDef:     tab,
			pd:         p,
			UpdateType: MetaAddTable,
		})
		return true
	}
}

func (p *pd) onMetaRemoveTable(replyer replyer, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaRemoveTable)
	var tab *db.TableDef
	var err error

	def := p.pState.Meta.Clone()

	err = func() error {

		if msg.Version != def.Version {
			return errors.New("version mismatch")
		}

		for _, v := range def.TableDefs {
			if v.Name == msg.Table {
				tab = v
				break
			}
		}

		if nil == tab {
			return errors.New("table not found")
		} else {
			return nil
		}
	}()

	if nil != err {
		replyer.reply(snet.MakeMessage(m.Context,
			&sproto.MetaRemoveTableResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, &sproto.MetaRemoveTableResp{}),
			},
			TabDef:     tab,
			pd:         p,
			UpdateType: MetaRemoveTable,
		})
		return true
	}
}

//向table添加fields
func (p *pd) onMetaAddFields(replyer replyer, m *snet.Message) bool {
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
				Name:         v.Name,
				Type:         v.Type,
				DefaultValue: v.Default,
				TabVersion:   tab.Version,
			})
		}

		if err := tab.Check(); nil != err {
			return err
		}

		dbc, err := sql.SqlOpen(p.config.DBConfig.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
		defer dbc.Close()

		if nil != err {
			return err
		}

		dbtab, err := sql.GetTableScheme(dbc, p.config.DBConfig.DBType, fmt.Sprintf("%s_%d", tab.Name, tab.DbVersion))
		if nil != err {
			return err
		} else if nil == dbtab {
			GetSugar().Errorf("table:%s in meta but not in db", tab.Name)
			//表不存在,不应该发生这种情况
			return sql.CreateTables(dbc, p.config.DBConfig.DBType, tab)
		} else {
			return sql.AddFields(dbc, p.config.DBConfig.DBType, &db.TableDef{
				Name:      tab.Name,
				DbVersion: tab.DbVersion,
				Fields:    tab.Fields[len(tab.Fields)-len(msg.Fields):],
			})
		}
	}()

	if nil != err {
		replyer.reply(snet.MakeMessage(m.Context,
			&sproto.MetaAddFieldsResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, &sproto.MetaAddFieldsResp{}),
			},
			TabDef:     tab,
			pd:         p,
			UpdateType: MetaAddFields,
		})
		return true
	}
}

func (p *pd) onMetaRemoveFields(replyer replyer, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaRemoveFields)
	var tab *db.TableDef
	var err error

	def := p.pState.Meta.Clone()

	err = func() error {

		if msg.Version != def.Version {
			return errors.New("version mismatch")
		}

		for _, v := range def.TableDefs {
			if v.Name == msg.Table {
				tab = v.Clone()
				break
			}
		}

		if nil == tab {
			return errors.New("table not found")
		}

		for _, v := range msg.Fields {
			if !tab.RemoveField(v) {
				return fmt.Errorf("field:%s not found", v)
			}
		}

		return nil

	}()

	if nil != err {
		replyer.reply(snet.MakeMessage(m.Context,
			&sproto.MetaRemoveFieldsResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return false
	} else {
		p.issueProposal(&ProposalUpdateMeta{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, &sproto.MetaRemoveFieldsResp{}),
			},
			TabDef:     tab,
			pd:         p,
			UpdateType: MetaRemoveFields,
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
		var fn func(replyer replyer, m *snet.Message) bool

		switch op.m.Msg.(type) {
		case *sproto.MetaAddTable:
			fn = p.onMetaAddTable
		case *sproto.MetaAddFields:
			fn = p.onMetaAddFields
		case *sproto.MetaRemoveTable:
			fn = p.onMetaRemoveTable
		case *sproto.MetaRemoveFields:
			fn = p.onMetaRemoveFields
		default:
		}

		if nil != fn && fn(op.replyer, op.m) {
			return
		} else {
			p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front())
		}

	}
}

func (p *pd) onUpdateMetaReq(replyer replyer, m *snet.Message) {
	p.metaUpdateQueue.PushBack(&metaOpration{
		replyer: replyer,
		m:       m,
	})
	p.processMetaUpdate()
}

func (p *pd) onGetScanTableMeta(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.GetScanTableMeta)
	resp := &sproto.GetScanTableMetaResp{}

	var tab *db.TableDef

	for _, v := range p.pState.Meta.TableDefs {
		if v.Name == msg.Table {
			tab = v
			break
		}
	}

	if nil == tab {
		resp.TabVersion = -1
	} else {
		resp.TabVersion = tab.DbVersion
		for _, v := range tab.Fields {
			resp.Fields = append(resp.Fields, &sproto.ScanField{
				Field:   v.Name,
				Version: v.TabVersion,
			})

		}
	}
	replyer.reply(snet.MakeMessage(m.Context, resp))
}
