package flypd

import (
	"container/list"
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

type DbMetaMgr struct {
	DbMeta      db.DbDef
	dbMetaBytes []byte
	opQueue     *list.List
	locked      bool
}

func (mgr *DbMetaMgr) clearOpQueue() {
	mgr.locked = false
	mgr.opQueue = list.New()
}

func (mgr *DbMetaMgr) lock() {
	if !mgr.locked {
		mgr.locked = true
	}
}

func (mgr *DbMetaMgr) unlock(pd *pd) {
	if mgr.locked {
		mgr.locked = false
		mgr.doOperation(pd)
	}
}

func (mgr *DbMetaMgr) pushOp(pd *pd, op *metaOpration) {
	mgr.opQueue.PushBack(op)
	mgr.doOperation(pd)
}

func (mgr *DbMetaMgr) doOperation(pd *pd) {
	if mgr.locked {
		return
	}

	for front := mgr.opQueue.Front(); nil != front; front = mgr.opQueue.Front() {

		op := mgr.opQueue.Remove(front).(*metaOpration)

		var fn func(replyer replyer, m *snet.Message) bool
		switch op.m.Msg.(type) {
		case *sproto.MetaAddTable:
			fn = pd.onMetaAddTable
		case *sproto.MetaAddFields:
			fn = pd.onMetaAddFields
		case *sproto.MetaRemoveTable:
			fn = pd.onMetaRemoveTable
		case *sproto.MetaRemoveFields:
			fn = pd.onMetaRemoveFields
		default:
		}

		if nil != fn && fn(op.replyer, op.m) {
			mgr.lock()
			return
		}
	}
}

func (p *pd) onGetMeta(replyer replyer, m *snet.Message) {
	replyer.reply(snet.MakeMessage(m.Context,
		&sproto.GetMetaResp{
			Version: p.DbMetaMgr.DbMeta.Version,
			Meta:    p.DbMetaMgr.dbMetaBytes,
		}))
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
	p.pd.mainque.AppendHighestPriotiryItem(func() {
		p.pd.DbMetaMgr.unlock(p.pd)
	})
}

func (p *ProposalInitMeta) apply(pd *pd) {
	pd.DbMetaMgr.DbMeta = *p.MetaDef
	pd.DbMetaMgr.dbMetaBytes, _ = p.MetaDef.ToJson()
	GetSugar().Infof("ProposalInitMeta apply version:%d", pd.DbMetaMgr.DbMeta.Version)
	pd.DbMetaMgr.unlock(pd)
}

func (p *pd) loadInitMeta() {
	GetSugar().Infof("loadInitMeta:%s", p.config.InitMetaPath)

	dbc, err := sql.SqlOpen(p.config.DBConfig.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
	defer dbc.Close()
	if nil != err {
		GetSugar().Panic(err)
	}

	p.DbMetaMgr.lock()
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

			missingTable := map[string]*db.TableDef{}

			for _, t := range def.TableDefs {
				tb, err := sql.GetTableScheme(dbc, p.config.DBConfig.DBType, t.Name)
				if nil != err {
					GetSugar().Panic(err)
				} else if nil != tb {

					t.DbVersion = tb.DbVersion

					//记录字段的最大版本
					fields := map[string]*db.FieldDef{}
					for _, v := range tb.Fields {
						if v.TabVersion > t.Version {
							t.Version = v.TabVersion
						}
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

					if t.DbVersion > def.Version {
						def.Version = t.DbVersion
					}
				} else {
					missingTable[t.Name] = t
				}
			}

			if def.Version == 0 {
				def.Version = 1
			}

			for _, t := range missingTable {
				t.Version = 1
				t.DbVersion = def.Version
				for _, field := range t.Fields {
					field.TabVersion = t.Version
				}

				err = sql.CreateTables(dbc, p.config.DBConfig.DBType, t)
				if nil != err {
					GetSugar().Panic(err)
				} else {
					GetSugar().Infof("create table:%s_%d ok", t.Name, t.DbVersion)
				}

			}

			p.issueProposal(&ProposalInitMeta{
				MetaDef: def,
				pd:      p,
			})
		}
	} else {
		p.issueProposal(&ProposalInitMeta{
			MetaDef: &db.DbDef{
				Version: 1,
			},
			pd: p,
		})
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
	p.pd.mainque.AppendHighestPriotiryItem(func() {
		p.pd.DbMetaMgr.unlock(p.pd)
	})
}

func (p *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalUpdateMeta, p)
}

func (p *ProposalUpdateMeta) apply(pd *pd) {
	def := &pd.DbMetaMgr.DbMeta
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

	pd.DbMetaMgr.dbMetaBytes, _ = def.ToJson()

	if pd.isLeader() {

		if nil != p.reply {
			p.reply(nil)
		}

		//notify all store leader
		for _, set := range pd.Deployment.Sets {
			for _, node := range set.Nodes {
				for storeID, store := range node.Store {
					if store.isLeader() {
						addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
						pd.udp.SendTo(addr, snet.MakeMessage(0,
							&sproto.NotifyUpdateMeta{
								Store:   int32(storeID),
								Version: pd.DbMetaMgr.DbMeta.Version,
								Meta:    pd.DbMetaMgr.dbMetaBytes,
							}))
					}
				}
			}
		}

		pd.DbMetaMgr.unlock(pd)
	}
}

func (p *pd) onMetaAddTable(replyer replyer, m *snet.Message) bool {
	msg := m.Msg.(*sproto.MetaAddTable)
	var tab *db.TableDef
	var err error

	def := p.DbMetaMgr.DbMeta.Clone()

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

	def := p.DbMetaMgr.DbMeta.Clone()

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
	def := p.DbMetaMgr.DbMeta.Clone()
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

	def := p.DbMetaMgr.DbMeta.Clone()

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

func (p *pd) onUpdateMetaReq(replyer replyer, m *snet.Message) {
	p.DbMetaMgr.pushOp(p, &metaOpration{
		replyer: replyer,
		m:       m,
	})
}

func (p *pd) onGetScanTableMeta(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.GetScanTableMeta)
	resp := &sproto.GetScanTableMetaResp{}

	var tab *db.TableDef

	for _, v := range p.DbMetaMgr.DbMeta.TableDefs {
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
