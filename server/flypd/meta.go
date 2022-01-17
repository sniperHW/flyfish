package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
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
}
