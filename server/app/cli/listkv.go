package main

import (
	"flag"
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/app/cli/ansicolor"
	"github.com/sniperHW/flyfish/server/flypd"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"sync/atomic"
	"time"
)

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

type idgen struct {
	counter int64
}

func (ig *idgen) Next() string {
	id := atomic.AddInt64(&ig.counter, 1)
	return fmt.Sprintf("%d", id)
}

var g_idgen idgen = idgen{}

type replica struct {
	id          string
	node        int
	status      string //leader,voter,learner
	progress    uint64 //副本同步进度
	raftID      uint64
	lastReport  int64
	metaVersion int64
	halt        bool
	service     string
}

type store struct {
	id        string
	storeID   int
	replica   []*replica
	isHalt    bool
	slotCount int
	kvcount   int
}

type set struct {
	id     string
	setID  int
	stores []*store
}

type kvstatus struct {
	kvcount           int
	freeslotCount     int
	transferSlotCount int
	selected          int
	sets              []*set
}

var g_kvstatus kvstatus

const (
	kvstatusHight = 2
	setsWidth     = 15 //sets控件的最大宽度
	nodeWidth     = 40
	nodeHight     = 6
)

func (k *kvstatus) show(g *gocui.Gui) {
	maxX, _ := g.Size()

	v, err := g.SetView("kvstatus", 0, 0, maxX-1, kvstatusHight)
	if nil != err {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		v.Title = "kvstatus"
		v.Highlight = true
	}

	v.Clear()

	fmt.Fprintf(v, "kvcount:%d freeslotCount:%d transferSlotCount:%d\n", k.kvcount, k.freeslotCount, k.transferSlotCount)

	k.showSets(g)
}

func (k *kvstatus) showSets(g *gocui.Gui) {
	_, maxY := g.Size()

	v, err := g.SetView("sets", 0, kvstatusHight, setsWidth+kvstatusHight, maxY-1)
	if nil != err {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		v.Title = "sets"
		v.Highlight = true
		v.SelBgColor = gocui.ColorGreen
		v.SelFgColor = gocui.ColorBlack
	}

	v.Clear()
	for _, s := range k.sets {
		fmt.Fprintf(v, "set:%d\n", s.setID)
	}

	if len(k.sets) > 0 {
		k.sets[k.selected].show(g)
	}
}

func (s *set) show(g *gocui.Gui) {
	maxX, maxY := g.Size()
	v, err := g.SetView(s.id, setsWidth, kvstatusHight, maxX-1, maxY-1)
	if nil != err {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		v.Title = fmt.Sprintf("set:%d", s.setID)
		v.Highlight = true
	}

	yy := kvstatusHight
	for _, v := range s.stores {
		yy = v.show(g, yy)
	}
}

func (s *set) clear(g *gocui.Gui) {
	for _, v := range s.stores {
		v.clear(g)
	}
}

func (s *store) clear(g *gocui.Gui) {
	for _, v := range s.replica {
		v.clear(g)
	}
	g.DeleteView(s.id)
}

func (s *store) show(g *gocui.Gui, y int) int {
	begY := y
	endY := y + nodeHight + 2
	maxX, _ := g.Size()

	v, err := g.SetView(s.id, setsWidth+1, begY, maxX-2, endY)

	if err != nil {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		v.Title = fmt.Sprintf("store:%d", s.storeID)
		v.Wrap = true
	}

	v.Clear()
	fmt.Fprintf(v, "kvcount:%d slotCount:%d\n", s.kvcount, s.slotCount)

	xx := 25
	for _, v := range s.replica {
		xx = v.show(g, xx, begY+2)
	}

	return endY + 2
}

func (r *replica) getShowStatus() string {
	if r.status == "leader" {
		return ansicolor.FillColor(ansicolor.Red, r.status)
	} else {
		return r.status
	}
}

func (r *replica) show(g *gocui.Gui, x, y int) int {
	begY := y
	endY := y + nodeHight
	begX := x
	endX := x + nodeWidth

	v, err := g.SetView(r.id, begX, begY, endX, endY)

	if err != nil {
		if err != gocui.ErrUnknownView {
			panic(err)
		}
		v.Wrap = true
		v.Title = fmt.Sprintf("node:%d", r.node)
	}

	now := time.Now().Unix()
	v.Clear()
	v.Write([]byte(fmt.Sprintf("service:%s\n", r.service)))
	v.Write([]byte(fmt.Sprintf("raftID:%x\n", r.raftID)))
	v.Write([]byte(fmt.Sprintf("status:%s\n", r.getShowStatus())))
	if now-r.lastReport > 60 {
		v.Write([]byte("progress:~\n"))
		v.Write([]byte("metaVersion:~\n"))
	} else {
		v.Write([]byte(fmt.Sprintf("progress:%d\n", r.progress)))
		v.Write([]byte(fmt.Sprintf("metaVersion:%d\n", r.metaVersion)))
	}

	return endX + 1
}

func (r *replica) clear(g *gocui.Gui) {
	g.DeleteView(r.id)
}

func onSelect(g *gocui.Gui, v *gocui.View) error {
	g.Update(func(g *gocui.Gui) error {
		var l string
		var err error
		_, cy := v.Cursor()
		if l, err = v.Line(cy); err != nil {
			l = ""
		}

		selected := 0

		for k, s := range g_kvstatus.sets {
			if l == fmt.Sprintf("set:%d", s.setID) {
				selected = k
			}
		}

		g_kvstatus.sets[g_kvstatus.selected].clear(g)
		g_kvstatus.selected = selected
		return nil
	})
	return nil
}

func layout(g *gocui.Gui) error {
	g_kvstatus.show(g)
	return nil
}

func getKvStatus(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{}); r != nil {
		var sets []*set
		resp := r.(*sproto.GetKvStatusResp)
		for _, s := range resp.Sets {
			set := &set{
				id:    g_idgen.Next(),
				setID: int(s.SetID),
			}

			for _, st := range s.GetStores() {
				store := &store{
					id:        g_idgen.Next(),
					storeID:   int(st.StoreID),
					slotCount: int(st.Slotcount),
					kvcount:   int(st.Kvcount),
				}
				for _, n := range s.Nodes {
					for _, nodeSt := range n.Stores {
						if st.StoreID == nodeSt.StoreID {
							replica := &replica{
								id:          g_idgen.Next(),
								node:        int(n.NodeID),
								lastReport:  n.LastReportTime,
								metaVersion: nodeSt.MetaVersion,
								halt:        nodeSt.Halt,
								progress:    nodeSt.Progress,
								raftID:      nodeSt.RaftID,
								service:     n.Service,
							}

							if nodeSt.IsLeader {
								replica.status = "leader"
							} else {
								switch nodeSt.Type {
								case int32(flypd.VoterStore):
									if nodeSt.Value == int32(flypd.FlyKvCommited) {
										replica.status = "voter"
									} else {
										replica.status = "voter(uncommited)"
									}
								case int32(flypd.RemoveStore):
									replica.status = "removing"
								case int32(flypd.LearnerStore):
									if nodeSt.Value == int32(flypd.FlyKvCommited) {
										replica.status = "learner"
									} else {
										replica.status = "learner(uncommited)"
									}
								}
							}
							store.replica = append(store.replica, replica)
						}
					}
				}

				sort.Slice(store.replica, func(l, r int) bool {
					return store.replica[l].node < store.replica[r].node
				})

				set.stores = append(set.stores, store)
			}

			sort.Slice(set.stores, func(l, r int) bool {
				return set.stores[l].storeID < set.stores[r].storeID
			})

			sets = append(sets, set)
		}

		sort.Slice(sets, func(l, r int) bool {
			return sets[l].setID < sets[r].setID
		})

		g.Update(func(g *gocui.Gui) error {
			for _, v := range g.Views() {
				if !(v.Name() == "sets" || v.Name() == "kvstatus") {
					g.DeleteView(v.Name())
				}
			}
			g_kvstatus.kvcount = int(resp.Kvcount)
			g_kvstatus.freeslotCount = int(resp.FreeSlotCount)
			g_kvstatus.transferSlotCount = int(resp.TransferSlotCount)
			g_kvstatus.sets = sets
			return nil
		})
	}
}

func main() {

	l := logger.NewZapLogger("listkv.log", "./log", "info", 1024*1024, 14, 14, false)

	logger.InitLogger(l)

	pdservice := flag.String("pdservice", "localhost:8111", "ip1:port1;ip2:port2;...")

	flag.Parse()

	go func() {
		http.ListenAndServe("localhost:8899", nil)
	}()

	g, err := gocui.NewGui(gocui.Output256)
	if err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	g.Cursor = true
	g.Mouse = true

	httpcli := consoleHttp.NewClient(*pdservice)

	g.SetManagerFunc(layout)

	go getKvStatus(httpcli, g)

	go func() {
		for {
			select {
			case <-time.After(1000 * time.Millisecond):
				getKvStatus(httpcli, g)
			}
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.MouseLeft, gocui.ModNone, onSelect); err != nil {
		panic(err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
}
