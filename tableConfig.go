package flyfish

import (
	"flyfish/conf"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func InitTableConfig() bool {
	var db *sqlx.DB
	var err error
	dbConfig := conf.DefConfig.DBConfig

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

		if !InitMeta(metas) {
			Errorln("InitMeta failed")
			return false
		}
	}

	return true
}
