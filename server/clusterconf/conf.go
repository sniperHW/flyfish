package clusterconf

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/server/slot"
)

var StorePerNode int = 5 //每节点store数量

type store struct {
	Id    int
	Slots *bitmap.Bitmap
}

type Node struct {
	ID          int
	HostIP      string
	RaftPort    int //raft端口
	ServicePort int //对外服务端口
	ConsolePort int //控制端口
	Stores      []store
}

//Nodes中的Node运行相同的store。每个store跟Nodes中的节点形成raftGroup
type RaftGroupJson struct {
	Nodes []int
}

type KvConfigJson struct {
	NodeInfo []Node
	Shard    []RaftGroupJson
}

func (this *KvConfigJson) getNode(id int) *Node {
	for i, v := range this.NodeInfo {
		if v.ID == id {
			return &this.NodeInfo[i]
		}
	}
	return nil
}

type KvConfig struct {
	Shard [][]*Node
}

func MarshalConfig(conf *KvConfigJson) ([]byte, error) {
	return json.Marshal(conf)
}

func UnmarshalConfig(b []byte) (*KvConfigJson, error) {
	var conf KvConfigJson
	if err := json.Unmarshal(b, &conf); nil != err {
		return nil, err
	} else {
		return &conf, nil
	}
}

/*
type RouteInfo struct {
	Store int
	Nodes []int
}

func MakeRoute(j *KvConfigJson) map[int]*RouteInfo {
	route := map[int]*RouteInfo{}
	storeCount := len(j.Shard) * StorePerNode
	var stores []RouteInfo
	for i := 0; i < storeCount; i++ {
		stores = append(stores, RouteInfo{
			Store: i + 1,
		})
	}

	for k, v := range j.Shard {
		for _, vv := range v.Nodes {
			for i := 0; i < StorePerNode; i++ {
				stores[k*StorePerNode+i].Nodes = append(stores[k*StorePerNode+i].Nodes, vv)
			}
		}
	}

	jj := 0
	for i := 0; i < slot.SlotCount; i++ {
		jj = (jj + 1) % storeCount
		route[i] = &stores[jj]
	}

	return route
}*/

func makeKvConfig(j *KvConfigJson) (*KvConfig, error) {
	storeCount := len(j.Shard) * StorePerNode
	var stores []*bitmap.Bitmap

	for i := 0; i < storeCount; i++ {
		stores = append(stores, bitmap.New(slot.SlotCount))
	}

	jj := 0
	for i := 0; i < slot.SlotCount; i++ {
		stores[jj].Set(i)
		jj = (jj + 1) % storeCount
	}

	var conf KvConfig

	for k, v := range j.Shard {

		shard := []*Node{}

		for _, vv := range v.Nodes {
			n := j.getNode(vv)
			for i := 0; i < StorePerNode; i++ {
				n.Stores = append(n.Stores, store{
					Id:    k*StorePerNode + i + 1,
					Slots: stores[k*StorePerNode+i],
				})
			}

			shard = append(shard, n)

		}

		conf.Shard = append(conf.Shard, shard)

	}

	return &conf, nil
}

func pgsqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)
	return sqlx.Open("postgres", connStr)
}

func mysqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname)
	return sqlx.Open("mysql", connStr)
}

func sqlOpen(sqlType string, host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	if sqlType == "mysql" {
		return mysqlOpen(host, port, dbname, user, password)
	} else {
		return pgsqlOpen(host, port, dbname, user, password)
	}
}

func LoadConfigFromDB(clusterID int, sqlType string, host string, port int, dbname string, user string, password string) (*KvConfig, error) {
	var db *sqlx.DB
	var err error

	db, err = sqlOpen(sqlType, host, port, dbname, user, password)

	if nil != err {
		return nil, err
	} else {

		rows, err := db.Query(fmt.Sprintf("select conf from kvconf where id = %d", clusterID))

		if nil != err {
			return nil, err
		}
		defer rows.Close()

		var str string

		if rows.Next() {
			err := rows.Scan(&str)
			if nil != err {
				return nil, err
			}
		}

		var confJson *KvConfigJson
		confJson, err = UnmarshalConfig([]byte(str))
		if nil != err {
			return nil, err
		}

		var conf *KvConfig
		conf, err = makeKvConfig(confJson)
		if nil != err {
			return nil, err
		} else {
			return conf, nil
		}
	}
}

func makeStoreMysql(clusterID int, oldVersion int, newVersion int, b []byte) string {
	c := string(b)
	return fmt.Sprintf("INSERT INTO kvconf(id,conf,version) VALUES (%d,'%s',%d) on duplicate key update conf='%s' , version=%d where id = %d and version=%d", clusterID, c, newVersion, c, clusterID, newVersion, oldVersion)
}

func makeStorePgsql(clusterID int, oldVersion int, newVersion int, b []byte) string {
	c := string(b)
	return fmt.Sprintf("INSERT INTO kvconf(id,conf,version) VALUES (%d,'%s',%d) ON conflict(id)  DO UPDATE SET conf='%s' , version=%d where kvconf.id = %d and kvconf.version=%d", clusterID, c, newVersion, c, newVersion, clusterID, oldVersion)
}

func LoadConfigJsonFromDB(clusterID int, sqlType string, host string, port int, dbname string, user string, password string) (*KvConfigJson, int, error) {
	var db *sqlx.DB
	var err error

	db, err = sqlOpen(sqlType, host, port, dbname, user, password)

	if nil != err {
		return nil, 0, err
	} else {

		rows, err := db.Query(fmt.Sprintf("select conf,version from kvconf where id = %d", clusterID))

		if nil != err {
			return nil, 0, err
		}
		defer rows.Close()

		var str string
		var version int

		if rows.Next() {
			err := rows.Scan(&str, &version)
			if nil != err {
				return nil, 0, err
			}
		}

		var confJson *KvConfigJson
		confJson, err = UnmarshalConfig([]byte(str))
		if nil != err {
			return nil, version, err
		} else {
			return confJson, version, nil
		}
	}
}

func StoreConfigJsonToDB(clusterID int, oldVersion int, newVersion int, sqlType string, host string, port int, dbname string, user string, password string, conf *KvConfigJson) error {
	b, err := MarshalConfig(conf)
	if nil != err {
		return err
	}

	var db *sqlx.DB
	db, err = sqlOpen(sqlType, host, port, dbname, user, password)
	if nil != err {
		return err
	}

	var sqlStr string
	if sqlType == "mysql" {
		sqlStr = makeStoreMysql(clusterID, oldVersion, newVersion, b)
	} else {
		sqlStr = makeStorePgsql(clusterID, oldVersion, newVersion, b)
	}

	_, err = db.Exec(sqlStr)

	return err
}
