package flydir

import (
	"github.com/gogo/protobuf/proto"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/server/clusterconf"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

func (d *dir) onGateReport(from *net.UDPAddr, m *sproto.GateReport) {
	d.mu.Lock()
	if int(m.ConfVersion) > d.kvconfVersion && !d.updateing {
		d.updateing = true
		go func() {
			var version int
			var err error
			config := d.config
			for {
				clusterConf := config.ClusterConfig
				_, version, err = clusterconf.LoadConfigJsonFromDB(clusterConf.ClusterID, clusterConf.DBType, clusterConf.DBHost, clusterConf.DBPort, clusterConf.ConfDB, clusterConf.DBUser, clusterConf.DBPassword)
				if nil == err {
					break
				}
			}
			d.mu.Lock()
			d.updateing = false
			d.kvconfVersion = version
			d.mu.Unlock()

		}()
	}

	g, ok := d.gates[m.Service]
	if !ok {
		reportVersion := int64(1)
		service := m.Service

		g := &gate{
			service:       m.Service,
			console:       m.Console,
			reportVersion: reportVersion,
		}

		var onTimeout func()

		onTimeout = func() {
			d.mu.Lock()
			if g.reportVersion == reportVersion {
				delete(d.gates, service)
			} else {
				//如果Reset失败，将会执行到这里，在这里设置新的定时器
				g.timer = time.AfterFunc(time.Second*sproto.PingTime, onTimeout)
			}
			d.mu.Unlock()
		}

		g.timer = time.AfterFunc(time.Second*sproto.PingTime, onTimeout)

		d.gates[service] = g

	} else {
		g.reportVersion++
		g.timer.Reset(time.Second * sproto.PingTime)
	}
	d.mu.Unlock()
}

func (d *dir) processConsoleMsg(from *net.UDPAddr, m proto.Message) {
	switch m.(type) {
	case *sproto.GateReport:
		d.onGateReport(from, m.(*sproto.GateReport))
	case *sproto.RemoveGate:
		d.mu.Lock()
		g, ok := d.gates[m.(*sproto.RemoveGate).Service]
		if ok {
			g.timer.Stop()
			delete(d.gates, m.(*sproto.RemoveGate).Service)
		}
		d.mu.Unlock()
	case *sproto.QueryGateList:
		gl := &sproto.GateList{}
		d.mu.Lock()
		for k, _ := range d.gates {
			gl.List = append(gl.List, k)
		}
		d.mu.Unlock()
		d.consoleConn.SendTo(from, gl)
	}
}

func (d *dir) initConsole(service string) error {
	var err error
	d.consoleConn, err = fnet.NewUdp(service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := d.consoleConn.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				d.processConsoleMsg(from, msg)
			}
		}
	}()

	return nil
}
