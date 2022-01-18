package flypd

import (
	//"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"os"
)

func (p *pd) checkMeta(meta []byte) (*db.DbDef, error) {
	def, err := db.MakeDbDefFromJsonString(meta)
	if nil != err {
		return nil, err
	}

	return def, def.Check()
}

func (p *pd) onGetMeta(from *net.UDPAddr, m *snet.Message) {
	version := int64(-1)
	var metaBytes []byte
	if nil != p.pState.Meta {
		metaBytes = p.pState.MetaBytes
		version = p.pState.Meta.Version
	}

	p.udp.SendTo(from, snet.MakeMessage(m.Context,
		&sproto.GetMetaResp{
			Version: version,
			Meta:    metaBytes,
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

			def, err := p.checkMeta(b)
			if nil != err {
				GetSugar().Panic(err)
			}

			dbc, err := sql.SqlOpen(p.config.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
			defer dbc.Close()

			if nil != err {
				GetSugar().Panic(err)
			}

			for _, v := range def.TableDefs {
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

			p.issueProposal(&ProposalInitMeta{
				MetaDef: def,
			})
		}
	}
}

type ProposalInitMeta struct {
	proposalBase
	MetaDef *db.DbDef
}

func (p *ProposalInitMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalInitMeta, p)
}

func (p *ProposalInitMeta) apply(pd *pd) {
	p.MetaDef.Version++
	for _, v := range p.MetaDef.TableDefs {
		v.Version++
	}
	pd.pState.Meta = p.MetaDef
	pd.pState.MetaBytes, _ = p.MetaDef.ToJson()
	GetSugar().Infof("ProposalInitMeta apply version:%d", pd.pState.Meta.Version)
}

func (p *ProposalInitMeta) replay(pd *pd) {
	p.apply(pd)
}

//添加一个table
func (p *pd) onMetaAddTable(from *net.UDPAddr, m *snet.Message) {

}

//向table添加fields
func (p *pd) onMetaAddFields(from *net.UDPAddr, m *snet.Message) {

}

/*
type ProposalUpdateMeta struct {
	proposalBase
	Msg     *sproto.UpdateMeta
	MetaDef *db.DbDef
}

func (p *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalUpdateMeta, p)
}

func (p *ProposalUpdateMeta) apply(pd *pd) {
	meta, err := func() (*db.DbDef, error) {
		var meta *db.DbDef
		if nil != p.Msg {
			if nil != pd.pState.Meta {
				meta = pd.pState.Meta.Clone()
			} else {
				meta, _ = db.MakeDbDefFromJsonString([]byte("{}"))
			}

			if meta.Version != p.Msg.Version {
				GetSugar().Infof("%v %v", meta.Version, p.Msg.Version)
				return nil, errors.New("version mismatch")
			}

			for _, v := range p.Msg.Updates {
				tb := meta.GetTableDef(v.Name)
				if nil == tb {
					tb = &db.TableDef{
						Name:      v.Name,
						DbVersion: meta.Version,
						Version:   0,
					}

					if err := meta.AddTable(tb); nil != err {
						return nil, err
					}
				}

				for _, vv := range v.Fields {
					if err := tb.AddField(&db.FieldDef{
						Name:        vv.Name,
						Type:        vv.Type,
						DefautValue: vv.Default,
					}); nil != err {
						return nil, err
					}
				}

				tb.Version++
			}
		} else {
			meta = p.MetaDef
			for _, v := range meta.TableDefs {
				v.Version++
			}
		}
		return meta, nil
	}()

	if nil == err {
		meta.Version++
		pd.pState.Meta = meta
		pd.pState.MetaBytes, _ = meta.ToJson()

		GetSugar().Infof("ProposalUpdateMeta apply version:%d", pd.pState.Meta.Version)

		//notify all store leader
		if nil != pd.pState.deployment {
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
	} else {
		GetSugar().Infof("ProposalUpdateMeta.apply err:%v", err)
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
	p.issueProposal(&ProposalUpdateMeta{
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(from, m, &sproto.UpdateMetaResp{}),
		},
		Msg: m.Msg.(*sproto.UpdateMeta),
	})
}*/
