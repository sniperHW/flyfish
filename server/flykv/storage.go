package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

type Storage interface {
	SaveMemberShip(lg *zap.Logger, cid types.ID, localID types.ID, data []byte) error
	LoadMemberShip(lg *zap.Logger, cid types.ID, localID types.ID) (*membership.MemberShip, error)
	SaveMeta(lg *zap.Logger, data []byte) error
	LoadMeta(lg *zap.Logger) (db.DBMeta, error)
}
