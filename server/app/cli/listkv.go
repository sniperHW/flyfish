package main

import (
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/server/app/cli/ansicolor"
	"github.com/sniperHW/flyfish/server/flypd"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sort"
	"time"
)

type viewKvstatus struct {
	kvcount           int
	freeslotCount     int
	transferSlotCount int
}

type listKvReplica struct {
	node        int
	status      string //leader,voter,learner
	progress    uint64 //副本同步进度
	raftID      uint64
	lastReport  int64
	metaVersion int64
	halt        bool
	service     string
}

type listKvStore struct {
	storeID   int
	replica   []*listKvReplica
	isHalt    bool
	slotCount int
	kvcount   int
}

type listKvSet struct {
	setID  int
	stores []*listKvStore
}

type listkv struct {
	kvcount           int
	freeslotCount     int
	transferSlotCount int
	selected          int
	sets              []*listKvSet
}

const (
	lkvstatusHight = 2
	lsetsWidth     = 15 //sets控件的最大宽度
	lnodeWidth     = 30
	lnodeHight     = 6
)

func (l *listkv) name() string {
	return "listkv"
}

func (l *listkv) canChangeView() bool {
	return true
}

func (l *listkv) getSelected() (int, *listKvSet) {
	for i, v := range l.sets {
		if v.setID == l.selected {
			return i, v
		}
	}

	if len(l.sets) > 0 {
		return 0, l.sets[0]
	} else {
		return -1, nil
	}
}

func (l *listkv) layoutReplica(g *gocui.Gui, set *listKvSet, store *listKvStore, r *listKvReplica, x int, y int) int {
	begY := y
	endY := y + lnodeHight
	begX := x
	endX := x + lnodeWidth

	v, err := g.SetView(fmt.Sprintf("set:%d-store:%d-node:%d", set.setID, store.storeID, r.node), begX, begY, endX, endY)

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
	if r.status == "leader" {
		v.Write([]byte(fmt.Sprintf("status:%s\n", ansicolor.FillColor(ansicolor.Red, r.status))))
	} else {
		v.Write([]byte(fmt.Sprintf("status:%s\n", r.status)))
	}
	if now-r.lastReport > 60 {
		v.Write([]byte("progress:~\n"))
		v.Write([]byte("metaVersion:~\n"))
	} else {
		v.Write([]byte(fmt.Sprintf("progress:%d\n", r.progress)))
		v.Write([]byte(fmt.Sprintf("metaVersion:%d\n", r.metaVersion)))
	}

	return endX + 1
}

func (l *listkv) layoutStore(g *gocui.Gui, set *listKvSet) {

	layout := func(store *listKvStore, y int) int {
		begY := y
		endY := y + lnodeHight + 2
		maxX, _ := g.Size()

		v, err := g.SetView(fmt.Sprintf("set:%d-store:%d", set.setID, store.storeID), lsetsWidth+1, begY, maxX-1, endY)

		if err != nil {
			if err != nil {
				if err != gocui.ErrUnknownView {
					panic(err)
				}
			}
			v.Title = fmt.Sprintf("store:%d", store.storeID)
			v.Wrap = true
		}

		v.Clear()
		fmt.Fprintf(v, "kvcount:%d slotCount:%d\n", store.kvcount, store.slotCount)

		xx := 25
		for _, v := range store.replica {
			xx = l.layoutReplica(g, set, store, v, xx, begY+2)
		}

		return endY + 2
	}

	yy := lkvstatusHight + 1

	for _, v := range set.stores {
		yy = layout(v, yy)
	}
}

func (l *listkv) layoutSets(g *gocui.Gui) {
	_, maxY := g.Size()

	v, err := g.SetView("listkv-sets", 0, lkvstatusHight+1, lsetsWidth, maxY-1)
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
		g.SetCurrentView("listkv-sets")

		l.keyBinding(g)
	}

	v.Clear()
	for _, s := range l.sets {
		fmt.Fprintf(v, "set:%d\n", s.setID)
	}

	if _, selected := l.getSelected(); selected != nil {
		l.layoutStore(g, selected)
	} else {
		v.MoveCursor(0, 0, false)
	}
}

func (l *listkv) layout(g *gocui.Gui) {
	maxX, _ := g.Size()

	v, err := g.SetView("listkv", 0, 0, maxX-1, lkvstatusHight)
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

	fmt.Fprintf(v, "kvcount:%d freeslotCount:%d transferSlotCount:%d\n", l.kvcount, l.freeslotCount, l.transferSlotCount)

	l.layoutSets(g)
}

func (l *listkv) clear(g *gocui.Gui) {
	for _, s := range l.sets {
		l.clearSet(g, s)
	}

	g.DeleteView("listkv-sets")
	g.DeleteView("listkv")
	l.deleteKeyBinding(g)
}

func (l *listkv) clearSet(g *gocui.Gui, set *listKvSet) {
	for _, st := range set.stores {
		for _, replica := range st.replica {
			g.DeleteView(fmt.Sprintf("set:%d-store:%d-node:%d", set.setID, st.storeID, replica.node))
		}
		g.DeleteView(fmt.Sprintf("set:%d-store:%d", set.setID, st.storeID))
	}
}

func (l *listkv) cursorMovement(d int) func(g *gocui.Gui, v *gocui.View) error {
	return func(g *gocui.Gui, v *gocui.View) error {
		if i, selected := l.getSelected(); nil != selected {
			next := i + d
			if next >= 0 && next < len(l.sets) {
				v.MoveCursor(0, d, false)
				l.clearSet(g, selected)
				l.selected = l.sets[next].setID
			}
		}
		return nil
	}
}

func (l *listkv) getKvStatus(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{}); r != nil {
		var sets []*listKvSet
		resp := r.(*sproto.GetKvStatusResp)
		for _, s := range resp.Sets {
			set := &listKvSet{
				setID: int(s.SetID),
			}

			for _, st := range s.GetStores() {
				store := &listKvStore{
					storeID:   int(st.StoreID),
					slotCount: int(st.Slotcount),
					kvcount:   int(st.Kvcount),
				}
				for _, n := range s.Nodes {
					for _, nodeSt := range n.Stores {
						if st.StoreID == nodeSt.StoreID {
							replica := &listKvReplica{
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
			for _, set := range l.sets {
				l.clearSet(g, set)
			}
			l.kvcount = int(resp.Kvcount)
			l.freeslotCount = int(resp.FreeSlotCount)
			l.transferSlotCount = int(resp.TransferSlotCount)
			l.sets = sets
			return nil
		})
	}
}

func (l *listkv) onActive(g *gocui.Gui, httpcli *consoleHttp.Client) {
	g.Highlight = false
	g.Cursor = false
	g.SelFgColor = gocui.ColorDefault
	l.getKvStatus(httpcli, g)
}

func (l *listkv) keyBinding(g *gocui.Gui) {
	if err := g.SetKeybinding("listkv-sets", gocui.KeyArrowUp, gocui.ModNone, l.cursorMovement(-1)); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("listkv-sets", gocui.KeyArrowDown, gocui.ModNone, l.cursorMovement(1)); err != nil {
		panic(err)
	}
}

func (l *listkv) deleteKeyBinding(g *gocui.Gui) {
	g.DeleteKeybindings("listkv-sets")
}

func (l *listkv) onTimeout(g *gocui.Gui, httpcli *consoleHttp.Client) {
	l.getKvStatus(httpcli, g)
}
