package flyfish

import (
	"encoding/binary"
	"flyfish/conf"
	"flyfish/proto"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"sync"

	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet"
)

var (
	crc64Table   *crc64.Table
	fileMtx      sync.Mutex
	backupFile   *os.File
	backFilePath string
)

const (
	size_head     = 5
	size_checksum = 8
)

func init() {
	fmt.Println("recover init")
	crc64Table = crc64.MakeTable(crc64.ISO)
}

func marshalRecord(r *record) *kendynet.ByteBuffer {

	var tt *proto.SqlType

	if r.writeBackFlag == write_back_insert {
		tt = proto.SqlType_insert.Enum()
	} else if r.writeBackFlag == write_back_update {
		tt = proto.SqlType_update.Enum()
	} else if r.writeBackFlag == write_back_delete {
		tt = proto.SqlType_insert.Enum()
	} else {
		Fatalln("[marshalRecord] invaild writeBackFlag", *r)
	}

	pbRecord := &proto.Record{
		Type:   tt,
		Table:  pb.String(r.table),
		Key:    pb.String(r.key),
		Fields: []*proto.Field{},
	}

	for _, v := range r.fields {
		pbRecord.Fields = append(pbRecord.Fields, v)
	}

	bytes, err := pb.Marshal(pbRecord)
	if err != nil {
		Fatalln("[marshalRecord]", err, *r)
	}

	/*
	 *  1字节标记记录是否有效
	 *  4字节数据和校验长度
	 */
	buffer := kendynet.NewByteBuffer(size_head + len(bytes) + size_checksum)
	buffer.AppendByte(byte(1))
	buffer.AppendUint32(uint32(len(bytes) + size_checksum))
	buffer.AppendBytes(bytes)
	checkSum := crc64.Checksum(bytes, crc64Table)
	buffer.AppendUint64(checkSum)

	return buffer

}

func backupRecord(r *record) {
	fileMtx.Lock()
	defer fileMtx.Unlock()

	config := conf.GetConfig()
	if nil == backupFile {
		backFilePath = config.BackDir + config.BackFile
		os.MkdirAll(config.BackDir, os.ModePerm)
		f, err := os.OpenFile(backFilePath, os.O_RDWR, os.ModePerm)
		if err != nil {
			if os.IsNotExist(err) {
				f, err = os.Create(backFilePath)
				if err != nil {
					Fatalln("[backupRecord]", err)
					return
				}
			} else {
				Fatalln("[backupRecord]", err)
				return
			}
		}
		backupFile = f
	}

	b := marshalRecord(r)
	if nil != b {
		backupFile.Write(b.Bytes())
		backupFile.Sync()
	}
}

func replayRecord(recoverUpdater *sqlUpdater, offset int64, data []byte) {

	var err error

	wOffset := offset - size_head

	checkSum := binary.BigEndian.Uint64(data[len(data)-size_checksum:])

	//校验数据
	if checkSum != crc64.Checksum(data[:len(data)-size_checksum], crc64Table) {
		Fatalln("invaild record : checkSum error ,offset:", offset)
	}

	pbRecord := &proto.Record{}

	if err = pb.Unmarshal(data[:len(data)-size_checksum], pbRecord); err != nil {
		Fatalln("replayRecord error ,offset:", offset, err)
	}

	fmt.Println(pbRecord)

	r := &record{
		key:    pbRecord.GetKey(),
		table:  pbRecord.GetTable(),
		fields: map[string]*proto.Field{},
	}

	meta := getMetaByTable(r.table)

	if nil == meta {
		Fatalln("replayRecord error invaild table ,offset:", offset, r.table)
	}

	for _, v := range pbRecord.GetFields() {
		r.fields[v.GetName()] = v
	}

	tt := pbRecord.GetType()
	if tt == proto.SqlType_insert {
		r.writeBackFlag = write_back_insert
		err = recoverUpdater.doInsert(r, meta)
	} else if tt == proto.SqlType_update {
		r.writeBackFlag = write_back_update
		err = recoverUpdater.doUpdate(r)
	} else if tt == proto.SqlType_delete {
		r.writeBackFlag = write_back_delete
		err = recoverUpdater.doDelete(r)
	} else {
		Fatalln("replayRecord invaild tt,offset:", offset)
	}

	if err != nil {
		Fatalln("replayRecord sqlError,offset:", offset, err)
	}

	//清除标记
	b := []byte{byte(0)}
	backupFile.WriteAt(b, wOffset)
	backupFile.Sync()

}

func doRecover(recoverUpdater *sqlUpdater) {
	rOffset := int64(0)
	head := make([]byte, size_head)
	ok := false
	defer func() {
		backupFile.Close()
		backupFile = nil
		//删除文件
		if ok {
			os.Remove(backFilePath)
		}
	}()

	for {
		count, err := backupFile.ReadAt(head, rOffset)
		if err == io.EOF {
			ok = true
			Infoln("Recover ok")
			return
		}
		if err != nil {
			panic(err)
		}
		if count != size_head {
			Fatalln("invaild record,offset", rOffset)
		}
		if head[0] == byte(0) {
			//无效数据，切换到下一条
			rOffset += (int64(binary.BigEndian.Uint32(head[1:size_head])) + int64(len(head)))
		} else {
			size := int(binary.BigEndian.Uint32(head[1:size_head]))
			rOffset += int64(len(head))
			data := make([]byte, size)
			count, err := backupFile.ReadAt(data, rOffset)
			if err != nil {
				Fatalln("invaild record,offset", rOffset)
			}
			if count != size {
				Fatalln("invaild record,offset", rOffset)
			}
			replayRecord(recoverUpdater, rOffset, data)
			rOffset += int64(size)
		}
	}
}

/*
*  从bak将未能写入到pgsql的操作写入到pgsql
 */

func Recover() {
	var db *sqlx.DB
	config := conf.GetConfig()
	dbConfig := config.DBConfig
	db, _ = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)

	recoverUpdater := newSqlUpdater(db, "recover", nil)

	backFilePath = config.BackDir + config.BackFile
	f, err := os.OpenFile(backFilePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			Errorln("Recover error:", err)
		}
	} else {
		backupFile = f
		doRecover(recoverUpdater)
	}
}
