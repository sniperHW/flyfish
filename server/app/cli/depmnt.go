package main

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func splitNode(str string) (nodeId int64, host string, servicePort int64, raftPort int64, err error) {
	n := strings.Split(str, " ")
	if len(n) != 4 {
		err = errors.New("invaild format")
		return
	}

	host = n[1]

	nodeId, err = strconv.ParseInt(n[0], 10, 32)
	if err != nil {
		return
	}

	servicePort, err = strconv.ParseInt(n[2], 10, 32)
	if err != nil {
		return
	}

	raftPort, err = strconv.ParseInt(n[3], 10, 32)
	if err != nil {
		return
	}

	return
}

type sceneDepmnt struct {
	scene        *ui.Scene
	setSelected  int32 //当前选中的setID
	nodeSelected int32 //当前选中的nodeID
	sets         []*sproto.Set
	httpcli      *consoleHttp.Client
}

func (sd *sceneDepmnt) getSelectedSet() (int, *sproto.Set) {
	for i, v := range sd.sets {
		if sd.setSelected == v.Id {
			return i, v
		}
	}

	if len(sd.sets) > 0 {
		return 0, sd.sets[0]
	} else {
		return 0, nil
	}
}

func (sd *sceneDepmnt) getSelectedNode() (int, *sproto.Node) {
	if _, s := sd.getSelectedSet(); s != nil {
		for i, v := range s.Nodes {
			if sd.nodeSelected == v.Id {
				return i, v
			}
		}

		if len(s.Nodes) > 0 {
			return 0, s.Nodes[0]
		}
	}
	return 0, nil
}

func (sd *sceneDepmnt) onActive(g *gocui.Gui, httpcli *consoleHttp.Client) {
	g.Highlight = true
	g.Cursor = false
	sd.getDepolyment(httpcli, g)
	sd.createLayer(g)
}

func (sd *sceneDepmnt) onTimeout(g *gocui.Gui, httpcli *consoleHttp.Client) {
	sd.getDepolyment(httpcli, g)
}

func (sd *sceneDepmnt) canChangeView() bool {
	return sd.scene.Len() == 1
}

func (sd *sceneDepmnt) clear(g *gocui.Gui) {
	sd.scene.Pop(g)
}

func (sd *sceneDepmnt) Layout(g *gocui.Gui) error {
	return sd.scene.Layout(g)
}

func (sd *sceneDepmnt) makeRemReq() (req proto.Message, resp proto.Message, err error) {
	switch sd.scene.Bottom().GetActiveView().Name {
	case "depmnt-sets":
		req, err = sd.makeRemSetReq()
		resp = &sproto.RemSetResp{}
	case "depmnt-nodes":
		req, err = sd.makeRemNodeReq()
		resp = &sproto.RemNodeResp{}
	default:
		err = errors.New("unknown view")
	}
	return
}

func (sd *sceneDepmnt) help(gui *gocui.Gui, helpMsg string) {
	if sd.scene.Top().Name != "help" {
		makeHelp(gui, sd.scene, helpMsg+"A: add set|node\nR: remove set|node\nC: mark clear set")
	}
}

func (sd *sceneDepmnt) makeRemNodeReq() (*sproto.RemNode, error) {
	if _, s := sd.getSelectedSet(); s == nil {
		return nil, errors.New("no set selected")
	} else {
		if _, n := sd.getSelectedNode(); n == nil {
			return nil, errors.New("no node selected")
		} else {
			return &sproto.RemNode{
				SetID:  s.Id,
				NodeID: n.Id,
			}, nil
		}
	}
}

func (sd *sceneDepmnt) makeRemSetReq() (*sproto.RemSet, error) {
	if _, s := sd.getSelectedSet(); s == nil {
		return nil, errors.New("no set selected")
	} else {
		return &sproto.RemSet{
			SetID: s.Id,
		}, nil
	}
}

func (sd *sceneDepmnt) makeAddReq(v *gocui.View) (req proto.Message, resp proto.Message, err error) {
	switch sd.scene.Bottom().GetActiveView().Name {
	case "depmnt-sets":
		req, err = sd.makeAddSetReq(v)
		resp = &sproto.AddSetResp{}
	case "depmnt-nodes":
		req, err = sd.makeAddNodeReq(v)
		resp = &sproto.AddNodeResp{}
	}
	return
}

func (sd *sceneDepmnt) makeAddSetReq(v *gocui.View) (*sproto.AddSet, error) {
	setID := int32(0)
	for i := 0; i < len(sd.sets); i++ {
		if sd.sets[i].Id-setID > int32(1) {
			break
		} else {
			setID = sd.sets[i].Id
		}
	}
	setID += int32(1)

	req := &sproto.AddSet{
		&sproto.DeploymentSet{
			SetID: setID,
		},
	}

	buffs := v.ViewBufferLines()
	for _, v := range buffs {
		if "" != v {
			if nodeId, host, servicePort, raftPort, err := splitNode(v); err != nil {
				return nil, err
			} else {
				req.Set.Nodes = append(req.Set.Nodes, &sproto.DeploymentKvnode{
					NodeID:      int32(nodeId),
					Host:        host,
					ServicePort: int32(servicePort),
					RaftPort:    int32(raftPort),
				})
			}
		}
	}

	if len(req.Set.Nodes) > 0 {
		return req, nil
	} else {
		return nil, errors.New("empty nodes")
	}
}

func (sd *sceneDepmnt) makeMarkClearReq(setID int32) (req proto.Message, resp proto.Message, err error) {
	return &sproto.SetMarkClear{
		SetID: setID,
	}, &sproto.SetMarkClearResp{}, nil
}

func (sd *sceneDepmnt) makeAddNodeReq(v *gocui.View) (*sproto.AddNode, error) {
	if _, s := sd.getSelectedSet(); s == nil {
		return nil, errors.New("no set selected")
	} else {
		buffs := v.ViewBufferLines()
		if len(buffs) == 0 || "" == buffs[0] {
			return nil, errors.New("no input")
		}

		if nodeId, host, servicePort, raftPort, err := splitNode(buffs[0]); err != nil {
			return nil, err
		} else {
			return &sproto.AddNode{
				SetID:       s.Id,
				NodeID:      int32(nodeId),
				Host:        host,
				ServicePort: int32(servicePort),
				RaftPort:    int32(raftPort),
			}, nil
		}
	}
}

func (sd *sceneDepmnt) getDepolyment(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetDeployment{}, &sproto.GetDeploymentResp{}); r != nil {
		resp := r.(*sproto.GetDeploymentResp)
		sort.Slice(resp.Sets, func(l, r int) bool {
			return resp.Sets[l].Id < resp.Sets[r].Id
		})

		for _, v := range resp.Sets {
			sort.Slice(v.Nodes, func(l, r int) bool {
				return v.Nodes[l].Id < v.Nodes[r].Id
			})
		}

		g.Update(func(g *gocui.Gui) error {
			sd.sets = resp.Sets
			return nil
		})
	}
}

func (sd *sceneDepmnt) createLayer(gui *gocui.Gui) {
	const (
		setsWidth = 15 //sets控件的最大宽度
		editWidth = 80
	)

	maxX, maxY := gui.Size()

	layer := &ui.Layer{
		Name: "depmnt",
	}

	cursorMovement := func(d int) func(_ *gocui.Gui, _ *gocui.View) error {
		return func(gui *gocui.Gui, v *gocui.View) error {
			switch sd.scene.Bottom().GetActiveView().Name {
			case "depmnt-sets":
				if i, s := sd.getSelectedSet(); nil != s {
					next := i + d
					if next >= 0 && next < len(sd.sets) {
						sd.setSelected = sd.sets[next].Id
					}
				}
			case "depmnt-nodes":
				if _, s := sd.getSelectedSet(); nil != s {
					if i, n := sd.getSelectedNode(); nil != n {
						next := i + d
						if next >= 0 && next < len(s.Nodes) {
							sd.nodeSelected = s.Nodes[next].Id
						}
					}
				}
			}
			return nil
		}
	}

	viewSets := &ui.View{
		Name:       "depmnt-sets",
		Title:      "sets",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			SelBgColor:   gocui.ColorGreen,
			SelFgColor:   gocui.ColorBlack,
			RightBottomX: setsWidth,
			RightBottomY: maxY - 1,
		},
		OutPut: func(v *gocui.View) {
			i, _ := sd.getSelectedSet()
			v.SetCursor(0, i)
			v.Clear()
			for _, s := range sd.sets {
				if s.MarkClear {
					fmt.Fprintf(v, "set:%d (Markclear)\n", s.Id)
				} else {
					fmt.Fprintf(v, "set:%d\n", s.Id)
				}
			}
		},
		OnViewCreate: func(gui *gocui.Gui, v *gocui.View) {
			gui.SetCurrentView("depmnt-sets")
		},
	}

	layer.AddView(viewSets)

	viewSets.AddKey(gocui.KeyArrowUp, cursorMovement(-1))
	viewSets.AddKey(gocui.KeyArrowDown, cursorMovement(1))
	viewSets.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		if sd.scene.Bottom().Name == "depmnt" {
			layer.ChangeActive()
		}
		return nil
	})

	viewNodes := &ui.View{
		Name:       "depmnt-nodes",
		Title:      "nodes",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			SelBgColor:   gocui.ColorGreen,
			SelFgColor:   gocui.ColorBlack,
			LeftTopX:     setsWidth + 1,
			RightBottomX: maxX - 1,
			RightBottomY: maxY - 1,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			if _, s := sd.getSelectedSet(); s != nil {
				for _, n := range s.Nodes {
					fmt.Fprintf(v, "node:%d host:%s servicePort:%d raftPort:%d\n", n.Id, n.Host, n.ServicePort, n.RaftPort)
				}
				i, _ := sd.getSelectedNode()
				v.SetCursor(0, i)
			} else {
				v.SetCursor(0, 0)
			}
		},
	}

	layer.AddView(viewNodes)

	viewNodes.AddKey(gocui.KeyArrowUp, cursorMovement(-1))
	viewNodes.AddKey(gocui.KeyArrowDown, cursorMovement(1))
	viewNodes.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		if sd.scene.Bottom().Name == "depmnt" {
			layer.ChangeActive()
		}
		return nil
	})

	remoteCall := func(name string, hint string, req proto.Message, resp proto.Message, err error) {
		if nil != err {
			ui.MakeMsg(gui, sd.scene, fmt.Sprintf("depmnt-%s-error", name), err.Error(), func() {
				sd.scene.Pop(gui)
			}, nil)
		} else {
			ui.MakeMsg(gui, sd.scene, fmt.Sprintf("depmnt-%s-wait-response", name), "Waitting response from server...", nil, nil)
			go func() {
				r, err := sd.httpcli.Call(req, resp)
				gui.Update(func(_ *gocui.Gui) error {
					sd.scene.Pop(gui)
					if nil != err {
						ui.MakeMsg(gui, sd.scene, fmt.Sprintf("depmnt-%s-error", name), err.Error(), func() {
							sd.scene.Pop(gui)
						}, nil)
					} else {
						vv := reflect.ValueOf(r).Elem()
						if vv.FieldByName("Ok").Interface().(bool) {
							ui.MakeMsg(gui, sd.scene, fmt.Sprintf("depmnt-%s-ok", name), "add Ok", func() {
								for sd.scene.Len() != 1 {
									sd.scene.Pop(gui)
								}
							}, nil)
						} else {
							ui.MakeMsg(gui, sd.scene, fmt.Sprintf("depmnt-%s-error", name), vv.FieldByName("Reason").Interface().(string), func() {
								sd.scene.Pop(gui)
							}, nil)
						}
					}
					return nil
				})
			}()
		}
	}

	onCltA := func(_ *gocui.Gui, _ *gocui.View) error {
		var name string
		var hint string
		switch sd.scene.Bottom().GetActiveView().Name {
		case "depmnt-sets":
			name = "AddSet"
			hint = fmt.Sprintf("node1 host1 servicePort1 raftPort1\nnode2 host2 servicePort2 raftPort2\n... ")
		case "depmnt-nodes":
			name = "AddNode"
			hint = fmt.Sprintf("node host servicePort raftPort")
		}

		ui.MakeEdit(gui, sd.scene, name, hint, func(editView *gocui.View) {
			req, resp, err := sd.makeAddReq(editView)
			remoteCall("add", hint, req, resp, err)
		})
		return nil
	}

	viewSets.AddKey('a', onCltA)
	viewNodes.AddKey('a', onCltA)

	onCltR := func(_ *gocui.Gui, _ *gocui.View) error {
		var hint string
		switch sd.scene.Bottom().GetActiveView().Name {
		case "depmnt-sets":
			if _, set := sd.getSelectedSet(); nil == set {
				return nil
			} else {
				hint = fmt.Sprintf("Do you really want to remove set:%d?", set.Id)
			}
		case "depmnt-nodes":
			if _, node := sd.getSelectedNode(); nil == node {
				return nil
			} else {
				hint = fmt.Sprintf("Do you really want to remove node:%d ?", node.Id)
			}
		}

		ui.MakeMsg(gui, sd.scene, "depmnt-remove-confirm", hint, func() {
			sd.scene.Pop(gui)
			req, resp, err := sd.makeRemReq()
			remoteCall("rem", hint, req, resp, err)
		}, func() {
			sd.scene.Pop(gui)
		})

		return nil
	}

	viewSets.AddKey('r', onCltR)
	viewNodes.AddKey('r', onCltR)

	onMarkclear := func(_ *gocui.Gui, _ *gocui.View) error {
		var hint string
		var set *sproto.Set
		if _, set = sd.getSelectedSet(); nil == set {
			return nil
		} else {
			hint = fmt.Sprintf("Do you really want to Markclear set:%d?", set.Id)
		}

		ui.MakeMsg(gui, sd.scene, "depmnt-markclear-confirm", hint, func() {
			sd.scene.Pop(gui)
			req, resp, err := sd.makeMarkClearReq(set.Id)
			remoteCall("Markclear", hint, req, resp, err)
		}, func() {
			sd.scene.Pop(gui)
		})
		return nil
	}

	viewSets.AddKey('c', onMarkclear)

	sd.scene.Push(layer)
}

func newDepmnt(gui *gocui.Gui, httpcli *consoleHttp.Client) *sceneDepmnt {
	sd := &sceneDepmnt{
		httpcli: httpcli,
		scene:   &ui.Scene{Name: "depmnt"},
	}
	return sd
}
