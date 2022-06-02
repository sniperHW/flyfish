package main

import (
	"fmt"
	"github.com/jroimartin/gocui"
	//"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/app/cli/ansicolor"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
	"github.com/sniperHW/flyfish/server/flypd"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sort"
	"time"
)

type replica struct {
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
	storeID   int
	replica   []*replica
	isHalt    bool
	slotCount int
	kvcount   int
}

type set struct {
	setID  int
	stores []*store
}

type sceneListKv struct {
	scene               *ui.Scene
	kvcount             int
	freeslotCount       int
	transferSlotCount   int
	selected            int
	sets                []*set
	viewStoreAndreplica []*ui.View
}

func (sc *sceneListKv) getSelected() (int, *set) {
	for i, v := range sc.sets {
		if v.setID == sc.selected {
			return i, v
		}
	}

	if len(sc.sets) > 0 {
		return 0, sc.sets[0]
	} else {
		return -1, nil
	}
}

func (sc *sceneListKv) onActive(g *gocui.Gui, httpcli *consoleHttp.Client) {
	g.Highlight = false
	g.Cursor = false
	//g.SelFgColor = gocui.ColorDefault
	sc.getKvStatus(httpcli, g)
	sc.createLayer(g)
}

func (sc *sceneListKv) onTimeout(g *gocui.Gui, httpcli *consoleHttp.Client) {
	sc.getKvStatus(httpcli, g)
}

func (sc *sceneListKv) canChangeView() bool {
	return sc.scene.Len() == 1
}

func (sc *sceneListKv) clear(g *gocui.Gui) {
	sc.scene.Pop(g)
	sc.viewStoreAndreplica = []*ui.View{}
}

func (sc *sceneListKv) Layout(g *gocui.Gui) error {
	return sc.scene.Layout(g)
}

func (sc *sceneListKv) help(gui *gocui.Gui) {
	if sc.scene.Top().Name != "help" {
		makeHelp(gui, sc.scene, "Ctrl-N change scene\n")
	}
}

func (sc *sceneListKv) getKvStatus(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{}); r != nil {
		var sets []*set
		resp := r.(*sproto.GetKvStatusResp)
		for _, s := range resp.Sets {
			set := &set{
				setID: int(s.SetID),
			}

			for _, st := range s.GetStores() {
				store := &store{
					storeID:   int(st.StoreID),
					slotCount: int(st.Slotcount),
					kvcount:   int(st.Kvcount),
				}
				for _, n := range s.Nodes {
					for _, nodeSt := range n.Stores {
						if st.StoreID == nodeSt.StoreID {
							replica := &replica{
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
			if bottom := sc.scene.Bottom(); bottom != nil {
				for _, v := range sc.viewStoreAndreplica {
					bottom.DeleteView(g, v.Name)
				}
				sc.viewStoreAndreplica = []*ui.View{}
			}
			sc.kvcount = int(resp.Kvcount)
			sc.freeslotCount = int(resp.FreeSlotCount)
			sc.transferSlotCount = int(resp.TransferSlotCount)
			sc.sets = sets
			return nil
		})
	}
}

func (sc *sceneListKv) createLayer(gui *gocui.Gui) {
	const (
		kvstatusHight = 2
		setsWidth     = 15 //sets控件的最大宽度
		nodeWidth     = 30
		nodeHight     = 6
	)

	maxX, maxY := gui.Size()

	layer := &ui.Layer{
		Name: "listkv",
	}

	viewKvStatus := &ui.View{
		Name:  "listkv-kvstatus",
		Title: "kvstatus",
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			RightBottomX: maxX - 1,
			RightBottomY: kvstatusHight,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintf(v, "kvcount:%d freeslotCount:%d transferSlotCount:%d\n", sc.kvcount, sc.freeslotCount, sc.transferSlotCount)
		},
	}

	layer.AddView(viewKvStatus)

	viewSets := &ui.View{
		Name:  "listkv-sets",
		Title: "sets",
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			SelBgColor:   gocui.ColorGreen,
			SelFgColor:   gocui.ColorBlack,
			LeftTopX:     0,
			LeftTopY:     kvstatusHight + 1,
			RightBottomX: setsWidth,
			RightBottomY: maxY - 1,
		},
		OutPut: func(v *gocui.View) {
			if _, s := sc.getSelected(); s == nil {
				v.SetCursor(0, 0)
			}
			v.Clear()
			for _, s := range sc.sets {
				fmt.Fprintf(v, "set:%d\n", s.setID)
			}
		},
		OnViewCreate: func(gui *gocui.Gui, v *gocui.View) {
			gui.SetCurrentView("listkv-sets")
		},
	}

	layer.AddView(viewSets)

	cursorMovement := func(d int) func(_ *gocui.Gui, _ *gocui.View) error {
		return func(gui *gocui.Gui, v *gocui.View) error {
			if i, selected := sc.getSelected(); nil != selected {
				next := i + d
				if next >= 0 && next < len(sc.sets) {
					v.MoveCursor(0, d, false)
					for _, vv := range sc.viewStoreAndreplica {
						layer.DeleteView(gui, vv.Name)
					}
					sc.viewStoreAndreplica = []*ui.View{}
					sc.selected = sc.sets[next].setID
				}
			}
			return nil
		}
	}

	viewSets.AddKey(gocui.KeyArrowUp, cursorMovement(-1))
	viewSets.AddKey(gocui.KeyArrowDown, cursorMovement(1))

	layer.BeforeLayout = func(gui *gocui.Gui) {
		if _, s := sc.getSelected(); s != nil {
			y := kvstatusHight + 1
			for _, st := range s.stores {
				store := st
				vStore := &ui.View{
					Name:  fmt.Sprintf("set:%d-store:%d", s.setID, store.storeID),
					Title: fmt.Sprintf("store:%d", store.storeID),
					Option: ui.UIOption{
						Wrap:         true,
						Highlight:    true,
						LeftTopX:     setsWidth + 1,
						LeftTopY:     y,
						RightBottomX: maxX - 1,
						RightBottomY: y + nodeHight + 3,
					},
					OutPut: func(v *gocui.View) {
						v.Clear()
						fmt.Fprintf(v, "kvcount:%d slotCount:%d\n", store.kvcount, store.slotCount)
					},
				}

				x := 25
				for _, r := range store.replica {
					replica := r
					vReplica := &ui.View{
						Name:  fmt.Sprintf("set:%d-store:%d-node:%d", s.setID, store.storeID, replica.node),
						Title: fmt.Sprintf("node:%d", replica.node),
						Option: ui.UIOption{
							Wrap:         true,
							Highlight:    true,
							LeftTopX:     x,
							LeftTopY:     y + 2,
							RightBottomX: x + nodeWidth,
							RightBottomY: y + nodeHight + 2,
							Z:            1,
						},
						OutPut: func(v *gocui.View) {
							v.Clear()
							now := time.Now().Unix()
							v.Clear()
							v.Write([]byte(fmt.Sprintf("service:%s\n", replica.service)))
							v.Write([]byte(fmt.Sprintf("raftID:%x\n", replica.raftID)))
							if replica.status == "leader" {
								v.Write([]byte(fmt.Sprintf("status:%s\n", ansicolor.FillColor(ansicolor.Red, replica.status))))
							} else {
								v.Write([]byte(fmt.Sprintf("status:%s\n", replica.status)))
							}
							if now-replica.lastReport > 60 {
								v.Write([]byte("progress:~\n"))
								v.Write([]byte("metaVersion:~\n"))
							} else {
								v.Write([]byte(fmt.Sprintf("progress:%d\n", replica.progress)))
								v.Write([]byte(fmt.Sprintf("metaVersion:%d\n", replica.metaVersion)))
							}
						},
					}

					sc.viewStoreAndreplica = append(sc.viewStoreAndreplica, vReplica)
					layer.AddView(vReplica)

					x += (nodeWidth + 2)
				}

				y += (nodeHight + 4)

				sc.viewStoreAndreplica = append(sc.viewStoreAndreplica, vStore)
				layer.AddView(vStore)
			}
		}
	}

	layer.AfterLayout = func(gui *gocui.Gui) {
		if sc.scene.Top().Name == "help" {
			gui.SetViewOnTop("help")
		}
	}

	sc.scene.Push(layer)
}

func newListKv(gui *gocui.Gui) *sceneListKv {
	s := &sceneListKv{
		scene: &ui.Scene{Name: "listkv"},
	}
	return s
}
