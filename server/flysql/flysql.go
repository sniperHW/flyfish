package flysql

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type replyer struct {
	session         *fnet.Socket
	seqno           int64
	totalPendingReq *int64
}

func (r *replyer) reply(resp *cs.RespMessage) {
	atomic.AddInt64(r.totalPendingReq, -1)
	if nil != r.session {
		r.session.Send(resp)
	}
}

func (r *replyer) dropReply() {
	atomic.AddInt64(r.totalPendingReq, -1)
}

type request struct {
	replyer  *replyer
	msg      *cs.ReqMessage
	deadline time.Time
}

type flysql struct {
	muC             sync.Mutex
	clients         map[*fnet.Socket]struct{}
	config          *Config
	dbc             *sqlx.DB
	listener        *cs.Listener
	closed          int32
	requestChan     chan *request
	totalPendingReq int64
	muMeta          sync.Mutex
	meta            db.DBMeta
	pdAddr          []*net.UDPAddr
	service         string
	scannerCount    int32
}

func (this *flysql) pushRequest(req *cs.ReqMessage, replyer *replyer) {
	atomic.AddInt64(replyer.totalPendingReq, 1)
	select {
	case this.requestChan <- &request{msg: req, replyer: replyer, deadline: time.Now().Add(time.Duration(req.Timeout) * time.Millisecond)}:
	default:
		GetSugar().Infof("dropReply 2")
		replyer.reply(&cs.RespMessage{
			Cmd:   req.Cmd,
			Seqno: req.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "flykv busy,please retry later"),
		})
	}
}

func (this *flysql) onClient(session *fnet.Socket) {

	go func() {
		this.muC.Lock()
		this.clients[session] = struct{}{}
		this.muC.Unlock()

		session.SetRecvTimeout(flyproto.PingTime * 10)
		//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
		session.SetInBoundProcessor(cs.NewReqInboundProcessor())
		session.SetEncoder(&cs.RespEncoder{})

		session.SetCloseCallBack(func(session *fnet.Socket, reason error) {
			this.muC.Lock()
			delete(this.clients, session)
			this.muC.Unlock()
		})

		session.BeginRecv(func(session *fnet.Socket, v interface{}) {
			if atomic.LoadInt32(&this.closed) == 1 {
				GetSugar().Infof("flysql closed")
				return
			}
			msg := v.(*cs.ReqMessage)
			switch msg.Cmd {
			case flyproto.CmdType_Ping:
				session.Send(&cs.RespMessage{
					Cmd:   msg.Cmd,
					Seqno: msg.Seqno,
					Data: &flyproto.PingResp{
						Timestamp: time.Now().UnixNano(),
					},
				})
			default:
				this.pushRequest(msg, &replyer{
					session:         session,
					seqno:           msg.Seqno,
					totalPendingReq: &this.totalPendingReq,
				})
			}
		})
	}()
}

func (this *flysql) startListener() {
	this.listener.Serve(this.onClient, this.onScanner)
}

func waitCondition(fn func() bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			if fn() {
				wg.Done()
				break
			}
		}
	}()
	wg.Wait()
}

func (this *flysql) Stop() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		this.listener.Close()

		waitCondition(func() bool {
			return atomic.LoadInt64(&this.totalPendingReq) == 0
		})

		//关闭现有连接
		this.muC.Lock()
		for c, _ := range this.clients {
			go c.Close(nil, time.Second*5)
		}
		this.muC.Unlock()

		waitCondition(func() bool {
			this.muC.Lock()
			defer this.muC.Unlock()
			return len(this.clients) == 0
		})

		close(this.requestChan)

		this.dbc.Close()
	}
}

var outputBufLimit fnet.OutputBufLimit = fnet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

func (this *flysql) heartbeat() {
	if atomic.LoadInt32(&this.closed) == 0 {
		r, _ := snet.UdpCall(this.pdAddr, &sproto.FlySqlHeartBeat{Service: this.service, MetaVersion: this.meta.GetVersion()}, &sproto.FlySqlHeartBeatResp{}, time.Second)
		if nil != r {
			if len(r.(*sproto.FlySqlHeartBeatResp).Meta) > 0 {
				def, _ := db.MakeDbDefFromJsonString(r.(*sproto.FlySqlHeartBeatResp).Meta)
				if nil == def {
					if meta, _ := sql.CreateDbMeta(def); nil != meta {
						this.muMeta.Lock()
						this.meta = meta
						this.muMeta.Unlock()
					}
				}
			}
		}
		time.AfterFunc(time.Second, this.heartbeat)
	}
}

func (this *flysql) start(service string) error {
	var err error

	this.service = service

	config := this.config

	pd := strings.Split(config.PD, ";")

	for _, v := range pd {
		addr, err := net.ResolveUDPAddr("udp", v)
		if nil != err {
			return err
		} else {
			this.pdAddr = append(this.pdAddr, addr)
		}
	}

	dbConfig := config.DBConfig

	this.dbc, err = sql.SqlOpen(dbConfig.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DB, dbConfig.User, dbConfig.Password)

	if nil != err {
		return err
	}

	defer func() {
		if nil != err {
			this.dbc.Close()
		}
	}()

	for {
		r, _ := snet.UdpCall(this.pdAddr, &sproto.FlySqlHeartBeat{Service: service}, &sproto.FlySqlHeartBeatResp{}, time.Second)
		if nil != r {
			def, err := db.MakeDbDefFromJsonString(r.(*sproto.FlySqlHeartBeatResp).Meta)
			if nil != def {
				this.meta, err = sql.CreateDbMeta(def)
			}
			if nil != err {
				return err
			}
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	this.listener, err = cs.NewListener("tcp", service, outputBufLimit)

	if nil != err {
		return err
	}

	this.startListener()

	time.AfterFunc(time.Second, this.heartbeat)

	GetSugar().Infof("flysql start:%s", service)

	return err
}

func NewFlysql(service string, config *Config) (*flysql, error) {

	flysql := &flysql{
		clients:     map[*fnet.Socket]struct{}{},
		config:      config,
		requestChan: make(chan *request, 10000),
	}

	if config.MaxScannerCount <= 0 {
		config.MaxScannerCount = 100
	}

	if err := flysql.start(service); nil == err {
		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				for request := range flysql.requestChan {
					if time.Now().After(request.deadline) {
						request.replyer.dropReply()
					} else {
						flysql.processRequest(request)
					}
				}
			}()
		}
		return flysql, nil
	} else {
		return nil, err
	}
}

func splitUniKey(unikey string) (table string, key string) {
	i := -1
	for k, v := range unikey {
		if v == 58 {
			i = k
			break
		}
	}

	if i >= 0 {
		table = unikey[:i]
		key = unikey[i+1:]
	}

	return
}

func (this *flysql) onGet(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.GetReq)

	wants := req.Fields
	if req.GetAll() {
		wants = tbmeta.GetAllFieldsName()
	}

	ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
	defer cancel()

	version, retFields, err := Load(context.TODO(), this.dbc, tbmeta.(*sql.TableMeta), key, wants, req.Version)
	select {
	case <-ctx.Done():
		request.replyer.dropReply()
	default:
		resp := &cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
		}

		pbdata := &flyproto.GetResp{}

		switch {
		case err == nil:
			pbdata.Fields = retFields
			pbdata.Version = proto.Int64(version)
		case err == ErrRecordNotExist:
			resp.Err = errcode.New(errcode.Errcode_record_notexist)
		case err == ErrRecordNotChange:
			resp.Err = errcode.New(errcode.Errcode_record_unchange)
		case err != nil:
			resp.Err = errcode.New(errcode.Errcode_error, err.Error())
		}

		resp.Data = pbdata

		request.replyer.reply(resp)
	}
}

func (this *flysql) onSet(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.SetReq)

	err := func() errcode.Error {

		if len(req.GetFields()) == 0 {
			return errcode.New(errcode.Errcode_error, "set fields is empty")
		}

		if err := tbmeta.CheckFields(req.GetFields()...); nil != err {
			return errcode.New(errcode.Errcode_error, err.Error())
		}

		return nil
	}()

	if nil != err {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   err,
		})
	} else {
		fields := map[string]*flyproto.Field{}
		for _, v := range req.Fields {
			fields[v.GetName()] = v
		}

		ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
		defer cancel()

		err := Set(context.TODO(), this.dbc, this.config.DBConfig.DBType, tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey), fields, req.Version)

		select {
		case <-ctx.Done():
			request.replyer.dropReply()
		default:
			resp := &cs.RespMessage{
				Cmd:   request.msg.Cmd,
				Seqno: request.msg.Seqno,
			}

			pbdata := &flyproto.SetResp{}

			if err == ErrVersionMismatch {
				resp.Err = errcode.New(errcode.Errcode_version_mismatch)
			} else if nil != err {
				resp.Err = errcode.New(errcode.Errcode_error, err.Error())
			}

			resp.Data = pbdata

			request.replyer.reply(resp)
		}
	}
}

func (this *flysql) onSetNx(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.SetNxReq)

	err := func() errcode.Error {

		if len(req.GetFields()) == 0 {
			return errcode.New(errcode.Errcode_error, "set fields is empty")
		}

		if err := tbmeta.CheckFields(req.GetFields()...); nil != err {
			return errcode.New(errcode.Errcode_error, err.Error())
		}

		return nil
	}()

	if nil != err {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   err,
		})
	} else {
		fields := map[string]*flyproto.Field{}
		for _, v := range req.Fields {
			fields[v.GetName()] = v
		}

		ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
		defer cancel()

		retFields, err := SetNx(context.TODO(), this.dbc, this.config.DBConfig.DBType, tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey), fields)

		select {
		case <-ctx.Done():
			request.replyer.dropReply()
		default:

			resp := &cs.RespMessage{
				Cmd:   request.msg.Cmd,
				Seqno: request.msg.Seqno,
			}

			pbdata := &flyproto.SetNxResp{}

			switch {
			case err == ErrRecordExist:
				pbdata.Fields = retFields
				resp.Err = errcode.New(errcode.Errcode_record_exist)
			case err != nil:
				resp.Err = errcode.New(errcode.Errcode_error, err.Error())
			}

			resp.Data = pbdata

			request.replyer.reply(resp)
		}

	}
}

func (this *flysql) onDel(key string, tbmeta db.TableMeta, request *request) {

	ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
	defer cancel()

	err := MarkDelete(context.TODO(), this.dbc, this.config.DBConfig.DBType, tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey))

	select {
	case <-ctx.Done():
		request.replyer.dropReply()
	default:

		resp := &cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
		}

		pbdata := &flyproto.DelResp{}

		switch {
		case err == ErrRecordNotExist:
			resp.Err = errcode.New(errcode.Errcode_record_notexist)
		case err != nil:
			resp.Err = errcode.New(errcode.Errcode_error, err.Error())
		}

		resp.Data = pbdata

		request.replyer.reply(resp)
	}
}

func (this *flysql) onCompareAndSet(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.CompareAndSetReq)

	err := func() errcode.Error {

		if req.New == nil {
			return errcode.New(errcode.Errcode_error, "new is nil")
		}

		if req.Old == nil {
			return errcode.New(errcode.Errcode_error, "old is nil")
		}

		if req.New.GetType() != req.Old.GetType() {
			return errcode.New(errcode.Errcode_error, "new and old in different type")
		}

		if err := tbmeta.CheckFields(req.New); nil != err {
			return errcode.New(errcode.Errcode_error, err.Error())
		}

		return nil
	}()

	if nil != err {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   err,
		})
	} else {

		ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
		defer cancel()

		retField, err := CompareAndSet(context.TODO(), this.dbc, this.config.DBConfig.DBType,
			tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey),
			req.Old, req.New)

		select {
		case <-ctx.Done():
			request.replyer.dropReply()
		default:

			resp := &cs.RespMessage{
				Cmd:   request.msg.Cmd,
				Seqno: request.msg.Seqno,
			}

			pbdata := &flyproto.CompareAndSetResp{}

			switch {
			case err == ErrCompareNotEqual:
				pbdata.Value = retField
				resp.Err = errcode.New(errcode.Errcode_cas_not_equal)
			case err == ErrRecordNotExist:
				resp.Err = errcode.New(errcode.Errcode_record_notexist)
			case err != nil:
				resp.Err = errcode.New(errcode.Errcode_error, err.Error())
			}

			resp.Data = pbdata

			request.replyer.reply(resp)
		}
	}
}

func (this *flysql) onCompareAndSetNx(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.CompareAndSetNxReq)

	err := func() errcode.Error {

		if req.New == nil {
			return errcode.New(errcode.Errcode_error, "new is nil")
		}

		if req.Old == nil {
			return errcode.New(errcode.Errcode_error, "old is nil")
		}

		if req.New.GetType() != req.Old.GetType() {
			return errcode.New(errcode.Errcode_error, "new and old in different type")
		}

		if err := tbmeta.CheckFields(req.New); nil != err {
			return errcode.New(errcode.Errcode_error, err.Error())
		}

		return nil
	}()

	if nil != err {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   err,
		})
	} else {

		ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
		defer cancel()

		retField, err := CompareAndSetNx(context.TODO(), this.dbc, this.config.DBConfig.DBType,
			tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey),
			req.Old, req.New)

		select {
		case <-ctx.Done():
			request.replyer.dropReply()
		default:
			resp := &cs.RespMessage{
				Cmd:   request.msg.Cmd,
				Seqno: request.msg.Seqno,
			}

			pbdata := &flyproto.CompareAndSetNxResp{}

			switch {
			case err == ErrCompareNotEqual:
				pbdata.Value = retField
				resp.Err = errcode.New(errcode.Errcode_cas_not_equal)
			case err != nil:
				resp.Err = errcode.New(errcode.Errcode_error, err.Error())
			}

			resp.Data = pbdata

			request.replyer.reply(resp)
		}
	}
}

func (this *flysql) onIncrBy(key string, tbmeta db.TableMeta, request *request) {
	req := request.msg.Data.(*flyproto.IncrByReq)

	err := func() errcode.Error {

		if nil == req.Field {
			return errcode.New(errcode.Errcode_error, "field is nil")
		}

		if !req.Field.IsInt() {
			return errcode.New(errcode.Errcode_error, "incrby accept int only")
		}

		if err := tbmeta.CheckFields(req.Field); nil != err {
			return errcode.New(errcode.Errcode_error, err.Error())
		}

		return nil
	}()

	if nil != err {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   err,
		})
	} else {

		ctx, cancel := context.WithTimeout(context.TODO(), request.deadline.Sub(time.Now()))
		defer cancel()

		retField, err := Add(context.TODO(), this.dbc, this.config.DBConfig.DBType,
			tbmeta.(*sql.TableMeta), key, sslot.Unikey2Slot(request.msg.UniKey), req.Field)

		select {
		case <-ctx.Done():
			request.replyer.dropReply()
		default:

			resp := &cs.RespMessage{
				Cmd:   request.msg.Cmd,
				Seqno: request.msg.Seqno,
			}

			pbdata := &flyproto.IncrByResp{}

			if err != nil {
				resp.Err = errcode.New(errcode.Errcode_error, err.Error())
			} else {
				pbdata.Field = retField
			}

			resp.Data = pbdata

			request.replyer.reply(resp)
		}

	}
}

func (this *flysql) processRequest(request *request) {
	table, key := splitUniKey(request.msg.UniKey)
	this.muMeta.Lock()
	tbmeta := this.meta.GetTableMeta(table)
	this.muMeta.Unlock()
	if nil == tbmeta {
		request.replyer.reply(&cs.RespMessage{
			Cmd:   request.msg.Cmd,
			Seqno: request.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "table not define"),
		})
	} else {
		switch request.msg.Cmd {
		case flyproto.CmdType_Get:
			this.onGet(key, tbmeta, request)
		case flyproto.CmdType_Set:
			this.onSet(key, tbmeta, request)
		case flyproto.CmdType_SetNx:
			this.onSetNx(key, tbmeta, request)
		case flyproto.CmdType_Del:
			this.onDel(key, tbmeta, request)
		case flyproto.CmdType_CompareAndSet:
			this.onCompareAndSet(key, tbmeta, request)
		case flyproto.CmdType_CompareAndSetNx:
			this.onCompareAndSetNx(key, tbmeta, request)
		case flyproto.CmdType_IncrBy:
			this.onIncrBy(key, tbmeta, request)
		}
	}

}
