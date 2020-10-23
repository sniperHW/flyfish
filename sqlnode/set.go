package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
)

type sqlTaskSet struct {
	sqlTaskBase
	cmd *cmdSet
}

func (t *sqlTaskSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskSet) do(db *sqlx.DB) {
	tableMeta := getDBMeta().getTableMeta(t.table)

}

type cmdSet struct {
	cmdBase
	fields  []*proto.Field
	version *int64
}

func (c *cmdSet) makeSqlTask() sqlTask {
	panic("implement me")
}

func (c *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	panic("implement me")
}

func onSet(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.SetReq)
}
