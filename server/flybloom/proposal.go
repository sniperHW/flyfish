package flybloom

import (
	"encoding/json"
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

const (
	proposalAdd = 1
	proposalNop = 2
)

type proposalBase struct {
	reply func(error)
}

type applyable interface {
	apply(fb *flybloom)
}

type replayable interface {
	replay(fb *flybloom)
}

func (p proposalBase) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
}

func (p proposalBase) OnMergeFinish(b []byte) []byte {
	return b
}

func serilizeProposal(b []byte, tt int, p interface{}) []byte {
	b = buffer.AppendByte(b, byte(tt))
	bb, err := json.Marshal(p)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

type ProposalNop struct {
	proposalBase
}

func (this *ProposalNop) OnError(err error) {
	GetSugar().Errorf("proposalNop error:%v", err)
}

func (this *ProposalNop) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalNop, this)
}

func (this *ProposalNop) apply(fb *flybloom) {
	GetSugar().Infof("ProposalNop.apply")
}

func (this *ProposalNop) replay(fb *flybloom) {

}

type ProposalAdd struct {
	proposalBase
	Hash []uint64
}

func (this *ProposalAdd) OnError(err error) {
	GetSugar().Errorf("ProposalAdd error:%v", err)
}

func (this *ProposalAdd) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAdd, this)
}

func (this *ProposalAdd) apply(fb *flybloom) {
	GetSugar().Infof("ProposalAdd.apply")
	fb.filter.AddWithHashs(this.Hash)
}

func (this *ProposalAdd) replay(fb *flybloom) {
	this.apply(fb)
}

func (fb *flybloom) replayProposal(proposal []byte) error {
	reader := buffer.NewReader(proposal)
	proposalType, err := reader.CheckGetByte()
	if nil != err {
		return err
	}

	var r replayable

	unmarshal := func(rr interface{}) (err error) {
		if err = json.Unmarshal(reader.GetAll(), rr); nil == err {
			r = rr.(replayable)
		}
		return
	}

	switch int(proposalType) {
	case proposalAdd:
		err = unmarshal(&ProposalAdd{})
	case proposalNop:
		err = unmarshal(&ProposalNop{})
	default:
		return errors.New("invaild proposal type")
	}

	if nil == err {
		r.replay(fb)
	}

	return err
}
