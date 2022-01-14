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
			if pd.pState.Meta.Version != p.Msg.Version {
				return nil, errors.New("version mismatch")
			}

			if nil != pd.pState.Meta.MetaDef {
				meta = pd.pState.Meta.MetaDef.Clone()
			} else {
				meta, _ = db.CreateDbDefFromJsonString([]byte("{}"))
			}

			for _, v := range p.Msg.Updates {
				tb := meta.GetTableDef(v.Name)
				if nil == tb {
					tb = &db.TableDef{
						Name: v.Name,
					}
					meta.TableDefs = append(meta.TableDefs, tb)
				}

				for _, vv := range v.Fields {
					tb.Fields = append(tb.Fields, &db.FieldDef{
						Name:        vv.Name,
						Type:        vv.Type,
						DefautValue: vv.Default,
					})
				}
			}
		} else {
			meta = p.MetaDef
		}
		return meta, checkDbDef(meta)
	}()

	if nil == err {
		pd.pState.Meta.Version++
		pd.pState.Meta.MetaDef = meta
		pd.pState.Meta.MetaBytes, _ = db.DbDefToJsonString(pd.pState.Meta.MetaDef)

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
									Version: int64(pd.pState.Meta.Version),
									Meta:    pd.pState.Meta.MetaBytes,
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
}
