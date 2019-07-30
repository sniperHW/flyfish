package flyfish

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
)

func LoadTableConfig() bool {
	var db *sqlx.DB
	var err error
	dbConfig := conf.GetConfig().DBConfig

	db, err = sqlOpen(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

	if nil != err {
		Errorln(err)
		return false
	} else {

		rows, err := db.Query("select __table__,__conf__ from table_conf")

		if nil != err {
			Errorln(err)
			return false
		}
		defer rows.Close()

		metas := []string{}

		for rows.Next() {
			var __table__ string
			var __conf__ string

			err := rows.Scan(&__table__, &__conf__)

			if nil != err {
				Errorln(err)
				return false
			}

			metas = append(metas, fmt.Sprintf("%s@%s", __table__, __conf__))
		}

		if !LoadMeta(metas) {
			Errorln("InitMeta failed")
			return false
		}
	}

	return true
}

func reloadTableConf(session kendynet.StreamSession, msg *codec.Message) {
	ok := LoadTableConfig()
	session.Send(&proto.ReloadTableConfResp{
		Ok: pb.Bool(ok),
	})
}
