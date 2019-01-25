package flyfish

import (
	"encoding/binary"
	"flyfish/conf"
	"flyfish/proto"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"hash/crc64"
	"io"
	"os"
	"sync"
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

/*
type record struct {
	writeBackFlag int
	key           string
	table         string
	uniKey        string
	ckey          *cacheKey
	fields        map[string]*proto.Field //所有命令的字段聚合
	expired       int64
	writeBackVer  int64
}

enum SqlType {
  insert   = 1;
  update   = 2;
  delete   = 3;
}

message record {
	required SqlType type = 2;
	required string table = 3;
	required string key = 4;
	repeated field  fields  = 5;
}

		if wb.writeBackFlag == write_back_update {
			err = this.doUpdate(wb)
		} else if wb.writeBackFlag == write_back_insert {
			err = this.doInsert(wb)
		} else if wb.writeBackFlag == write_back_delete {

*/

func marshalRecord(r *record) *kendynet.ByteBuffer {

	var tt *proto.SqlType

	if r.writeBackFlag == write_back_insert {
		tt = proto.SqlType_insert.Enum()
	} else if r.writeBackFlag == write_back_update {
		tt = proto.SqlType_update.Enum()
	} else if r.writeBackFlag == write_back_delete {
		tt = proto.SqlType_insert.Enum()
	} else {
		panic("invaild writeBackFlag")
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
		panic(err)
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
	if nil == backupFile {
		backFilePath = fmt.Sprintf("%s/flyfish_backup.bak", conf.BackDir)
		os.MkdirAll(conf.BackDir, os.ModePerm)
		f, err := os.OpenFile(backFilePath, os.O_RDWR, os.ModePerm)
		if err != nil {
			if os.IsNotExist(err) {
				f, err = os.Create(backFilePath)
				if err != nil {
					panic(err)
					return
				}
			} else {
				panic(err)
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

func replayRecord(offset int64, data []byte) {
	wOffset := offset - size_head

	checkSum := binary.BigEndian.Uint64(data[len(data)-size_checksum:])

	//校验数据
	if checkSum != crc64.Checksum(data[:len(data)-size_checksum], crc64Table) {
		panic("invaild record:checkSum error")
	}

	pbRecord := &proto.Record{}

	if err := pb.Unmarshal(data[:len(data)-size_checksum], pbRecord); err != nil {
		panic(err)
	}

	fmt.Println(pbRecord)

	//清除标记
	b := []byte{byte(0)}
	backupFile.WriteAt(b, wOffset)
	backupFile.Sync()

}

func doRecover() {
	rOffset := int64(0)
	head := make([]byte, size_head)
	for {
		count, err := backupFile.ReadAt(head, rOffset)
		if err == io.EOF {
			backupFile.Close()
			backupFile = nil
			//删除文件
			os.Remove(backFilePath)
			fmt.Println("EOF")
			return
		}
		if err != nil {
			panic(err)
		}
		if count != size_head {
			panic("invaild record")
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
				panic(err)
			}
			if count != size {
				panic("invaild record")
			}
			replayRecord(rOffset, data)
			rOffset += int64(size)
		}
	}
}

/*
*  从bak将未能写入到pgsql的操作写入到pgsql
 */

func Recover() {
	backFilePath = fmt.Sprintf("%s/flyfish_backup.bak", conf.BackDir)
	f, err := os.OpenFile(backFilePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			panic(err)
		}
	} else {
		backupFile = f
		doRecover()
	}
}
