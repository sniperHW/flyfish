package membership

import (
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"go.uber.org/zap"
)

type Storage interface {
	SaveMemberShip(lg *zap.Logger, cid types.ID, localID types.ID, data []byte) error
	LoadMemberShip(lg *zap.Logger, cid types.ID, localID types.ID) (*MemberShip, error)
}
