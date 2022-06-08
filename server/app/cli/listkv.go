package main

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/app/cli/ansicolor"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
	"github.com/sniperHW/flyfish/server/flypd"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"reflect"
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
	setID     int
	markClear bool
	stores    []*store
}

type sceneListKv struct {
	scene               *ui.Scene
	kvcount             int
	freeslotCount       int
	transferSlotCount   int
	selected            int
	sets                []*set
	viewStoreAndreplica []*ui.View
	httpcli             *consoleHttp.Client
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
	g.Highlight = true
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

func (sc *sceneListKv) help(gui *gocui.Gui, helpMsg string) {
	if sc.scene.Top().Name != "help" {
		makeHelp(gui, sc.scene, helpMsg+"C: show control panncel\nEsc: close control panncel")
	}
}

func (sc *sceneListKv) getKvStatus(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{}); r != nil {
		var sets []*set
		resp := r.(*sproto.GetKvStatusResp)
		for _, s := range resp.Sets {
			set := &set{
				setID:     int(s.SetID),
				markClear: s.MarkClear,
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
								lastReport:  nodeSt.LastReport,
								metaVersion: nodeSt.MetaVersion,
								halt:        nodeSt.Halt,
								progress:    nodeSt.Progress,
								raftID:      nodeSt.RaftID,
								service:     n.Service,
							}

							if nodeSt.IsLeader {
								replica.status = "leader"
							} else {
								switch nodeSt.StoreType {
								case int32(flypd.VoterStore):
									replica.status = "voter"
								case int32(flypd.RemovingStore):
									replica.status = "removing"
								case int32(flypd.LearnerStore):
									replica.status = "learner"
								case int32(flypd.AddLearnerStore):
									replica.status = "learner(uncommited)"
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

func (sc *sceneListKv) makeControlPannel(gui *gocui.Gui) {
	maxX, maxY := gui.Size()

	var (
		width   = 10
		height  = 7
		btWidth = 18
	)

	layer := &ui.Layer{
		Name: "listkv-control-pannel",
	}

	btnSuspendKvStore := &ui.View{
		Name:       "listkv-control-pannel-suspend",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, ui.CenterPrint(btWidth, "SuspendKvStore"))
		},
	}

	btnResumeKvStore := &ui.View{
		Name:       "listkv-control-pannel-resume",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, ui.CenterPrint(btWidth, "ResumeKvStore"))
		},
	}

	btnClearKvCache := &ui.View{
		Name:       "listkv-control-pannel-clearcache",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, ui.CenterPrint(btWidth, "ClearKvCache"))
		},
	}

	btnClearDbdata := &ui.View{
		Name:       "listkv-control-pannel-clear-dbdata",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, ui.CenterPrint(btWidth, "ClearDbdata"))
		},
	}

	boundView := &ui.View{
		Name:  "listkv-control-pannel",
		Title: "Control-Pannel",
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
		},
	}

	boundView.Option.LeftTopX = (maxX / 2) - width
	boundView.Option.LeftTopY = (maxY / 2) - height
	boundView.Option.RightBottomX = (maxX / 2) + width
	boundView.Option.RightBottomY = (maxY / 2) + height

	btnSuspendKvStore.Option.LeftTopX = boundView.Option.LeftTopX + 1
	btnSuspendKvStore.Option.LeftTopY = boundView.Option.LeftTopY + 1
	btnSuspendKvStore.Option.RightBottomX = btnSuspendKvStore.Option.LeftTopX + btWidth
	btnSuspendKvStore.Option.RightBottomY = btnSuspendKvStore.Option.LeftTopY + 2

	btnResumeKvStore.Option.LeftTopX = boundView.Option.LeftTopX + 1
	btnResumeKvStore.Option.LeftTopY = btnSuspendKvStore.Option.RightBottomY + 1
	btnResumeKvStore.Option.RightBottomX = btnResumeKvStore.Option.LeftTopX + btWidth
	btnResumeKvStore.Option.RightBottomY = btnResumeKvStore.Option.LeftTopY + 2

	btnClearKvCache.Option.LeftTopX = boundView.Option.LeftTopX + 1
	btnClearKvCache.Option.LeftTopY = btnResumeKvStore.Option.RightBottomY + 1
	btnClearKvCache.Option.RightBottomX = btnClearKvCache.Option.LeftTopX + btWidth
	btnClearKvCache.Option.RightBottomY = btnClearKvCache.Option.LeftTopY + 2

	btnClearDbdata.Option.LeftTopX = boundView.Option.LeftTopX + 1
	btnClearDbdata.Option.LeftTopY = btnClearKvCache.Option.RightBottomY + 1
	btnClearDbdata.Option.RightBottomX = btnClearDbdata.Option.LeftTopX + btWidth
	btnClearDbdata.Option.RightBottomY = btnClearDbdata.Option.LeftTopY + 2

	layer.AddView(boundView)
	layer.AddView(btnSuspendKvStore)
	layer.AddView(btnResumeKvStore)
	layer.AddView(btnClearKvCache)
	layer.AddView(btnClearDbdata)
	layer.SetActiveByName(btnSuspendKvStore.Name)

	btnSuspendKvStore.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnSuspendKvStore.AddKey(gocui.KeyEsc, func(g *gocui.Gui, vv *gocui.View) error {
		sc.scene.Pop(g)
		return nil
	})

	btnResumeKvStore.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnResumeKvStore.AddKey(gocui.KeyEsc, func(g *gocui.Gui, vv *gocui.View) error {
		sc.scene.Pop(g)
		return nil
	})

	btnClearKvCache.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnClearKvCache.AddKey(gocui.KeyEsc, func(g *gocui.Gui, vv *gocui.View) error {
		sc.scene.Pop(g)
		return nil
	})

	btnClearDbdata.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnClearDbdata.AddKey(gocui.KeyEsc, func(g *gocui.Gui, vv *gocui.View) error {
		sc.scene.Pop(g)
		return nil
	})

	onBtnEnter := func(name string, makeReq func() (proto.Message, proto.Message), hint string) func(*gocui.Gui, *gocui.View) error {
		return func(_ *gocui.Gui, _ *gocui.View) error {
			ui.MakeMsg(gui, sc.scene, fmt.Sprintf("listkv-%s-confirm", name), hint, func() {
				sc.scene.Pop(gui)
				req, resp := makeReq()

				ui.MakeMsg(gui, sc.scene, fmt.Sprintf("listkv-%s-wait-response", name), "Waitting response from server...", nil, nil)
				go func() {
					r, err := sc.httpcli.Call(req, resp)
					gui.Update(func(_ *gocui.Gui) error {
						sc.scene.Pop(gui)
						if nil != err {
							ui.MakeMsg(gui, sc.scene, fmt.Sprintf("listkv-%s-error", name), err.Error(), func() {
								sc.scene.Pop(gui)
							}, nil)
						} else {
							vv := reflect.ValueOf(r).Elem()
							if vv.FieldByName("Ok").Interface().(bool) {
								ui.MakeMsg(gui, sc.scene, fmt.Sprintf("listkv-%s-ok", name), "Ok", func() {
									sc.scene.Pop(gui)
								}, nil)
							} else {
								ui.MakeMsg(gui, sc.scene, fmt.Sprintf("listkv-%s-error", name), vv.FieldByName("Reason").Interface().(string), func() {
									sc.scene.Pop(gui)
								}, nil)
							}
						}
						return nil
					})
				}()
			}, func() {
				sc.scene.Pop(gui)
			})

			return nil
		}
	}

	btnSuspendKvStore.AddKey(gocui.KeyEnter, onBtnEnter("SuspendKvStore", func() (proto.Message, proto.Message) {
		return &sproto.SuspendKvStore{}, &sproto.SuspendKvStoreResp{}
	}, "Do you really want to SuspendKvStore?"))

	btnResumeKvStore.AddKey(gocui.KeyEnter, onBtnEnter("ResumeKvStore", func() (proto.Message, proto.Message) {
		return &sproto.ResumeKvStore{}, &sproto.ResumeKvStoreResp{}
	}, "Do you really want to ResumeKvStore?"))

	btnClearKvCache.AddKey(gocui.KeyEnter, onBtnEnter("ClearKvCache", func() (proto.Message, proto.Message) {
		return &sproto.ClearCache{}, &sproto.ClearCacheResp{}
	}, "Do you really want to btnClearKvCache?"))

	btnClearDbdata.AddKey(gocui.KeyEnter, onBtnEnter("ClearDbdata", func() (proto.Message, proto.Message) {
		return &sproto.ClearDBData{}, &sproto.ClearDBDataResp{}
	}, "Do you really want to ClearDbdata?"))

	sc.scene.Push(layer)

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
		Name:       "listkv-sets",
		Title:      "sets",
		SelectAble: true,
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
				if s.markClear {
					fmt.Fprintf(v, "set:%d (Markclear)\n", s.setID)
				} else {
					fmt.Fprintf(v, "set:%d\n", s.setID)
				}
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

	viewSets.AddKey('c', func(gui *gocui.Gui, _ *gocui.View) error {
		logger.GetSugar().Infof("len:%d", sc.scene.Len())
		if sc.scene.Len() == 1 {
			sc.makeControlPannel(gui)
		}
		return nil
	})

	sc.scene.Push(layer)
}

func newListKv(gui *gocui.Gui, httpcli *consoleHttp.Client) *sceneListKv {
	s := &sceneListKv{
		scene:   &ui.Scene{Name: "listkv"},
		httpcli: httpcli,
	}
	return s
}
