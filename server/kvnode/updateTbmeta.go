package kvnode

import (
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

type tbMetaProposal struct {
	metaJson []byte
	meta     db.DBMeta
	store    *kvstore
}

func (this *tbMetaProposal) Isurgent() bool {
	return true
}

func (this *tbMetaProposal) OnError(err error) {
	GetSugar().Errorf("tbMetaProposal error:%v", err)
}

func (this *tbMetaProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_tbmeta))
	b = buffer.AppendInt32(b, int32(len(this.metaJson)))
	b = buffer.AppendBytes(b, this.metaJson)
	return b
}

func (this *tbMetaProposal) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		c := getCompressor()
		cb, err := c.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
		releaseCompressor(c)
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
}

func (this *tbMetaProposal) apply() {
	this.store.meta = this.meta
	for _, v := range this.store.keyvals {
		for _, vv := range v.kv {
			vv.tbmeta = this.store.meta.GetTableMeta(vv.tbmeta.TableName())
		}
	}
}
