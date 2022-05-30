package main

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jroimartin/gocui"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const (
	dpsetsWidth = 15 //sets控件的最大宽度
	dpeditWidth = 80
)

type depmnt struct {
	setSelected    int32 //当前选中的setID
	nodeSelected   int32 //当前选中的nodeID
	sets           []*sproto.Set
	httpcli        *consoleHttp.Client
	activeMainView int
	mainViews      []string //{"depmnt-sets","depmnt-nodes"}
	stack          []view
}

type viewTips struct {
	msg  string
	keys map[interface{}]func()
}

func (vt *viewTips) name() string {
	return "depmnt-tips"
}

func (vt *viewTips) layout(g *gocui.Gui) {
	maxX, _ := g.Size()
	v, err := g.SetView(vt.name(), (maxX-40)/2, 10, (maxX-40)/2+40, 20)
	if err != nil {
		if err != gocui.ErrUnknownView {
			panic(err)
		}
		v.Wrap = true
		vt.keyBinding(g)
	}
	g.Cursor = false
	v.Clear()
	fmt.Fprintf(v, vt.msg)
}

func (vt *viewTips) clear(g *gocui.Gui) {
	g.DeleteView(vt.name())
	vt.deleteKeyBinding(g)
}

func (vt *viewTips) onActive(*gocui.Gui, *consoleHttp.Client) {
	return
}

func (vt *viewTips) onTimeout(*gocui.Gui, *consoleHttp.Client) {
	return
}

func (vt *viewTips) canChangeView() bool {
	return false
}

func (vt *viewTips) keyBinding(g *gocui.Gui) {
	for k, _ := range vt.keys {
		key := k
		if err := g.SetKeybinding(vt.name(), key, gocui.ModNone, func(_ *gocui.Gui, _ *gocui.View) error {
			if fn, ok := vt.keys[key]; ok {
				fn()
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}
}

func (vt *viewTips) deleteKeyBinding(g *gocui.Gui) {
	g.DeleteKeybindings(vt.name())
}

type viewAdd struct {
	depmnt *depmnt
}

func (va *viewAdd) name() string {
	return "depmnt-add"
}

func (va *viewAdd) layout(g *gocui.Gui) {
	maxX, maxY := g.Size()

	v, err := g.SetView(va.name(), (maxX-dpeditWidth)/2, 1, (maxX-dpeditWidth)/2+dpeditWidth, maxY-1)
	if err != nil {
		if err != gocui.ErrUnknownView {
			panic(err)
		}

		if va.depmnt.mainViews[va.depmnt.activeMainView] == "depmnt-sets" {
			v.Title = "addSet (editable)"
		} else {
			v.Title = "addNode (editable)"
		}

		v.Editable = true
		v.Wrap = true
		va.keyBinding(g)
	}

	g.Cursor = true
}

func (va *viewAdd) clear(g *gocui.Gui) {
	g.DeleteView(va.name())
	va.deleteKeyBinding(g)
}

func (va *viewAdd) onActive(*gocui.Gui, *consoleHttp.Client) {
	return
}

func (va *viewAdd) onTimeout(*gocui.Gui, *consoleHttp.Client) {
	return
}

func (va *viewAdd) canChangeView() bool {
	return false
}

func (va *viewAdd) keyBinding(g *gocui.Gui) {
	if err := g.SetKeybinding(va.name(), gocui.KeyCtrlO, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		req, resp, confirmMsg, err := va.depmnt.makeAddReq(v)
		va.depmnt.showConfirm(g, req, resp, confirmMsg, err)
		return nil
	}); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding(va.name(), gocui.KeyCtrlA, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		va.depmnt.popStackTop(g)
		return nil
	}); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding(va.name(), gocui.KeyEsc, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		va.depmnt.popStackTop(g)
		return nil
	}); err != nil {
		panic(err)
	}
}

func (va *viewAdd) deleteKeyBinding(g *gocui.Gui) {
	g.DeleteKeybindings(va.name())
}

func (d *depmnt) getStackTop() view {
	if len(d.stack) > 0 {
		return d.stack[len(d.stack)-1]
	} else {
		return nil
	}
}

func (d *depmnt) popStackTop(g *gocui.Gui) {
	if top := d.getStackTop(); nil != top {
		top.clear(g)
		d.stack[len(d.stack)-1] = nil
		d.stack = d.stack[:len(d.stack)-1]
	}
}

func (d *depmnt) clearStack(g *gocui.Gui) {
	for len(d.stack) > 0 {
		d.popStackTop(g)
	}
}

func (d *depmnt) getSelectedSet() (int, *sproto.Set) {
	for i, v := range d.sets {
		if d.setSelected == v.Id {
			return i, v
		}
	}

	if len(d.sets) > 0 {
		return 0, d.sets[0]
	} else {
		return -1, nil
	}
}

func (d *depmnt) getSelectedNode() (int, *sproto.Node) {
	if _, s := d.getSelectedSet(); s != nil {
		for i, v := range s.Nodes {
			if d.nodeSelected == v.Id {
				return i, v
			}
		}

		if len(s.Nodes) > 0 {
			return 0, s.Nodes[0]
		}
	}
	return -1, nil
}

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

func (d *depmnt) makeAddReq(v *gocui.View) (req proto.Message, resp proto.Message, confirmMsg string, err error) {
	switch d.mainViews[d.activeMainView] {
	case "depmnt-sets":
		req, confirmMsg, err = d.makeAddSetReq(v)
		resp = &sproto.AddSetResp{}
	case "depmnt-nodes":
		req, confirmMsg, err = d.makeAddNodeReq(v)
		resp = &sproto.AddNodeResp{}
	default:
		err = errors.New("unknown view")
	}
	return
}

func (d *depmnt) makeAddSetReq(v *gocui.View) (*sproto.AddSet, string, error) {
	setID := int32(0)
	for i := 0; i < len(d.sets); i++ {
		if d.sets[i].Id-setID > int32(1) {
			break
		} else {
			setID = d.sets[i].Id
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
				return nil, "", err
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
		return req, "do you really want to add set?", nil
	} else {
		return nil, "", errors.New("empty nodes")
	}
}

func (d *depmnt) makeAddNodeReq(v *gocui.View) (*sproto.AddNode, string, error) {
	if _, s := d.getSelectedSet(); s == nil {
		return nil, "", errors.New("no set selected")
	} else {
		buffs := v.ViewBufferLines()
		if len(buffs) == 0 || "" == buffs[0] {
			return nil, "", errors.New("no input")
		}

		if nodeId, host, servicePort, raftPort, err := splitNode(buffs[0]); err != nil {
			return nil, "", err
		} else {
			return &sproto.AddNode{
				SetID:       s.Id,
				NodeID:      int32(nodeId),
				Host:        host,
				ServicePort: int32(servicePort),
				RaftPort:    int32(raftPort),
			}, "do you really want to add node?", nil
		}
	}
}

func (d *depmnt) getDepolyment(cli *consoleHttp.Client, g *gocui.Gui) {
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
			d.sets = resp.Sets
			return nil
		})
	}
}

func (d *depmnt) layout(g *gocui.Gui) {
	maxX, maxY := g.Size()

	//首先显示主视图
	vSets, err := g.SetView("depmnt-sets", 0, 0, dpsetsWidth, maxY-1)
	if nil != err {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		vSets.Title = "sets"
		vSets.Highlight = true
		vSets.Wrap = true
		vSets.SelBgColor = gocui.ColorGreen
		vSets.SelFgColor = gocui.ColorBlack
	}

	vSets.Clear()
	for _, s := range d.sets {
		fmt.Fprintf(vSets, "set:%d\n", s.Id)
	}

	vNode, err := g.SetView("depmnt-nodes", dpsetsWidth, 0, maxX-1, maxY-1)
	if nil != err {
		if err != nil {
			if err != gocui.ErrUnknownView {
				panic(err)
			}
		}
		vNode.Title = "nodes"
		vNode.Highlight = true
		vNode.Wrap = true
		vNode.SelBgColor = gocui.ColorGreen
		vNode.SelFgColor = gocui.ColorBlack

		d.keyBinding(g)
	}
	vNode.Clear()

	if i, s := d.getSelectedSet(); s != nil {
		vSets.SetCursor(0, i)
		for _, v := range s.Nodes {
			fmt.Fprintf(vNode, "node:%d host:%s servicePort:%d raftPort:%d\n", v.Id, v.Host, v.ServicePort, v.RaftPort)
		}

		if i, n := d.getSelectedNode(); n != nil {
			vNode.SetCursor(0, i)
		} else {
			vNode.SetCursor(0, 0)
		}

	} else {
		vSets.SetCursor(0, 0)
		vNode.SetCursor(0, 0)
	}

	g.Cursor = false

	if len(d.stack) > 0 {
		for _, v := range d.stack {
			v.layout(g)
		}
		setCurrentViewOnTop(g, d.stack[len(d.stack)-1].name())
	} else {
		setCurrentViewOnTop(g, d.mainViews[d.activeMainView])
	}
}

func (d *depmnt) name() string {
	return "depmnt"
}

func (d *depmnt) clear(g *gocui.Gui) {
	d.clearStack(g)
	g.DeleteView("depmnt-sets")
	g.DeleteView("depmnt-nodes")
	d.deleteKeyBinding(g)
}

func (d *depmnt) onActive(g *gocui.Gui, cli *consoleHttp.Client) {
	g.Highlight = true
	g.Cursor = false
	g.SelFgColor = gocui.ColorGreen
	d.getDepolyment(cli, g)
	return
}

func (d *depmnt) onTimeout(g *gocui.Gui, cli *consoleHttp.Client) {
	d.getDepolyment(cli, g)
	return
}

func (d *depmnt) canChangeView() bool {
	return len(d.stack) == 0
}

func (d *depmnt) onCtrlA(g *gocui.Gui, v *gocui.View) error {
	d.stack = append(d.stack, &viewAdd{
		depmnt: d,
	})
	return nil
}

func (d *depmnt) makeRemReq(v *gocui.View) (req proto.Message, resp proto.Message, confirmMsg string, err error) {
	switch d.mainViews[d.activeMainView] {
	case "depmnt-sets":
		req, confirmMsg, err = d.makeRemSetReq()
		resp = &sproto.RemSetResp{}
	case "depmnt-nodes":
		req, confirmMsg, err = d.makeRemNodeReq()
		resp = &sproto.RemNodeResp{}
	default:
		err = errors.New("unknown view")
	}
	return
}

func (d *depmnt) makeRemNodeReq() (*sproto.RemNode, string, error) {
	if _, s := d.getSelectedSet(); s == nil {
		return nil, "", errors.New("no set selected")
	} else {
		if _, n := d.getSelectedNode(); n == nil {
			return nil, "", errors.New("no node selected")
		} else {
			return &sproto.RemNode{
				SetID:  s.Id,
				NodeID: n.Id,
			}, fmt.Sprintf("do you really want to remove node:%d", n.Id), nil
		}
	}
}

func (d *depmnt) makeRemSetReq() (*sproto.RemSet, string, error) {
	if _, s := d.getSelectedSet(); s == nil {
		return nil, "", errors.New("no set selected")
	} else {
		return &sproto.RemSet{
			SetID: s.Id,
		}, fmt.Sprintf("do you really want to remove set:%d", s.Id), nil
	}
}

func (d *depmnt) showConfirm(g *gocui.Gui, req proto.Message, resp proto.Message, confirmMsg string, err error) {
	tips := &viewTips{
		keys: map[interface{}]func(){},
	}

	tips.keys[gocui.KeyEsc] = func() {
		d.popStackTop(g)
	}

	if nil != err {
		tips.msg = fmt.Sprintf("error:%s\npress Esc to close window!\n", err.Error())
	} else {
		tips.msg = confirmMsg + "\nPress Enter to confirm operation!\npress Esc to cancel!"
		tips.keys[gocui.KeyEsc] = func() {
			d.popStackTop(g)
		}

		tips.keys[gocui.KeyEnter] = func() {
			tips.msg = "waitting server response...!"
			tips.keys[gocui.KeyEnter] = func() {}
			tips.keys[gocui.KeyEsc] = func() {}
			go func() {
				r, err := d.httpcli.Call(req, resp)
				g.Update(func(g *gocui.Gui) error {
					if nil != err {
						tips.msg = fmt.Sprintf("error:%s\nPress Esc to close window!\n", err.Error())
						tips.keys[gocui.KeyEsc] = func() {
							d.popStackTop(g)
						}
					} else {
						vv := reflect.ValueOf(r).Elem()
						if vv.FieldByName("Ok").Interface().(bool) {
							tips.msg = "OK!\nPress Esc to close window!"
							tips.keys[gocui.KeyEsc] = func() {
								d.clearStack(g)
							}
						} else {
							tips.msg = fmt.Sprintf("error:%s\nPress Esc to close window!\n", vv.FieldByName("Reason").Interface().(string))
							tips.keys[gocui.KeyEsc] = func() {
								d.popStackTop(g)
							}
						}
					}
					return nil
				})
			}()
		}
	}

	d.stack = append(d.stack, tips)
}

func (d *depmnt) onCtrlR(g *gocui.Gui, v *gocui.View) error {
	req, resp, confirmMsg, err := d.makeRemReq(v)
	d.showConfirm(g, req, resp, confirmMsg, err)
	return nil
}

func (d *depmnt) cursorMovement(dd int) func(g *gocui.Gui, v *gocui.View) error {
	return func(g *gocui.Gui, v *gocui.View) error {
		if nil != d.getStackTop() {
			return nil
		}

		if i, s := d.getSelectedSet(); nil != s {
			switch d.mainViews[d.activeMainView] {
			case "depmnt-sets":
				next := i + dd
				if next >= 0 && next < len(d.sets) {
					d.setSelected = d.sets[next].Id
					d.nodeSelected = 0
					if len(d.sets[next].Nodes) > 0 {
						d.nodeSelected = d.sets[next].Nodes[0].Id
					}
				}
			case "depmnt-nodes":
				if i, n := d.getSelectedNode(); nil != n {
					next := i + dd
					if next >= 0 && next < len(s.Nodes) {
						d.nodeSelected = s.Nodes[next].Id
					}
				}
			default:
			}
		}
		return nil
	}
}

func (d *depmnt) changeMainView(g *gocui.Gui, v *gocui.View) error {
	d.activeMainView = (d.activeMainView + 1) % len(d.mainViews)
	return nil
}

func (d *depmnt) keyBinding(g *gocui.Gui) {
	if err := g.SetKeybinding("depmnt-sets", gocui.KeyCtrlA, gocui.ModNone, d.onCtrlA); err != nil {
		panic(err)
	}
	if err := g.SetKeybinding("depmnt-nodes", gocui.KeyCtrlA, gocui.ModNone, d.onCtrlA); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-sets", gocui.KeyCtrlR, gocui.ModNone, d.onCtrlR); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-nodes", gocui.KeyCtrlR, gocui.ModNone, d.onCtrlR); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-sets", gocui.KeyArrowUp, gocui.ModNone, d.cursorMovement(-1)); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-sets", gocui.KeyArrowDown, gocui.ModNone, d.cursorMovement(1)); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-nodes", gocui.KeyArrowUp, gocui.ModNone, d.cursorMovement(-1)); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-nodes", gocui.KeyArrowDown, gocui.ModNone, d.cursorMovement(1)); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-sets", gocui.KeyTab, gocui.ModNone, d.changeMainView); err != nil {
		panic(err)
	}

	if err := g.SetKeybinding("depmnt-nodes", gocui.KeyTab, gocui.ModNone, d.changeMainView); err != nil {
		panic(err)
	}

}

func (d *depmnt) deleteKeyBinding(g *gocui.Gui) {
	g.DeleteKeybindings("depmnt-sets")
	g.DeleteKeybindings("depmnt-nodes")
}
