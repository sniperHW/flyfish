package flyfish

import (
	"flyfish/conf"
	"fmt"
	"github.com/jmoiron/sqlx"
)

func InitTableConfig() bool {

	//db, err := pgOpen(conf.ConfDbHost, conf.ConfDbPort, conf.ConfDataBase, conf.ConfDbUser, conf.ConfDbPassword)

	var db *sqlx.DB
	var err error
	if conf.SqlType == "pgsql" {
		db, _ = pgOpen(conf.DbHost, conf.DbPort, conf.DbDataBase, conf.DbUser, conf.DbPassword)
	} else {
		db, _ = mysqlOpen(conf.DbHost, conf.DbPort, conf.DbDataBase, conf.DbUser, conf.DbPassword)
	}

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
