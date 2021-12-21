package membership

import (
	"go.uber.org/zap"
)

type Storage interface {
	SaveMemberShip(lg *zap.Logger, data []byte) error
	LoadMemberShip(lg *zap.Logger) (*MemberShip, error)
	IsKeyNotFound(err error) bool
}
