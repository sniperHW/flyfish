package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"log"
	"net/http"
	_ "net/http/pprof"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	setsWidth = 15 //sets控件的最大宽度
	editWidth = 80
)

type stackItem struct {
	view    string
	onEnter func()
	onQ     func()
	msg     string
}

type app struct {
	setSelected    int32 //当前选中的setID
	nodeSelected   int32 //当前选中的nodeID
	sets           []*sproto.Set
	httpcli        *consoleHttp.Client
	activeMainView int
	mainViews      []string //{"sets","nodes"}
	stack          []*stackItem
}

func (a *app) getStackTop() *stackItem {
	if len(a.stack) > 0 {
		return a.stack[len(a.stack)-1]
	} else {
		return nil
	}
}

func (a *app) popStackTop(g *gocui.Gui) {
	if top := a.getStackTop(); nil != top {
		g.DeleteView(top.view)
		a.stack[len(a.stack)-1] = nil
		a.stack = a.stack[:len(a.stack)-1]
	}
}

func (a *app) clearStack(g *gocui.Gui) {
	for len(a.stack) > 0 {
		a.popStackTop(g)
	}
}

func (a *app) getSelectedSet() (int, *sproto.Set) {
	for i, v := range a.sets {
		if a.setSelected == v.Id {
			return i, v
		}
	}

	if len(a.sets) > 0 {
		return 0, a.sets[0]
	} else {
		return -1, nil
	}
}

func (a *app) getSelectedNode() (int, *sproto.Node) {
	if _, s := a.getSelectedSet(); s != nil {
		for i, v := range s.Nodes {
			if a.nodeSelected == v.Id {
				return i, v
			}
		}

		if len(s.Nodes) > 0 {
			return 0, s.Nodes[0]
		}
	}
	return -1, nil
}

func setCurrentViewOnTop(g *gocui.Gui, name string) (*gocui.View, error) {
	if _, err := g.SetCurrentView(name); err != nil {
		return nil, err
	}
	return g.SetViewOnTop(name)
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

func (a *app) makeAddSetReq(v *gocui.View) (*sproto.AddSet, error) {
	setID := int32(0)
	for i := 0; i < len(a.sets); i++ {
		if a.sets[i].Id-setID > int32(1) {
			break
		} else {
			setID = a.sets[i].Id
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

func (a *app) makeAddNodeReq(v *gocui.View) (*sproto.AddNode, error) {
	if _, s := a.getSelectedSet(); s == nil {
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

func (a *app) onCtrlSpace(g *gocui.Gui, v *gocui.View) error {
	//只有当栈顶是"add"时才执行
	logger.GetSugar().Infof("onCtrlSpace")
	if top := a.getStackTop(); nil != top && top.view == "add" {
		var req proto.Message
		var resp proto.Message
		var err error
		switch a.mainViews[a.activeMainView] {
		case "sets":
			req, err = a.makeAddSetReq(v)
			resp = &sproto.AddSetResp{}
		case "nodes":
			req, err = a.makeAddNodeReq(v)
			resp = &sproto.AddNodeResp{}
		default:
			return errors.New("unknown view")
		}

		st := &stackItem{
			view: "confirm",
			msg:  "press enter to confirm operation!\npress q to cancel!",
			onQ:  func() { a.popStackTop(g) },
		}
		if nil == err {
			st.onEnter = func() {
				a.popStackTop(g)
				st := &stackItem{
					view: "operation",
					msg:  "waitting server response...!",
				}
				a.stack = append(a.stack, st)

				go func() {
					r, err := a.httpcli.Call(req, resp)
					g.Update(func(g *gocui.Gui) error {
						if top := a.getStackTop(); nil != top && top.view == "operation" {
							top.onQ = func() {
								a.popStackTop(g)
							}
							if nil != err {
								top.msg = fmt.Sprintf("error:%s\npress q to close window!\n", err.Error())
							} else {
								vv := reflect.ValueOf(r).Elem()
								if vv.FieldByName("Ok").Interface().(bool) {
									top.onQ = func() {
										a.clearStack(g)
									}
									top.msg = "OK!\npress q to close window!"
								} else {
									top.msg = fmt.Sprintf("error:%s\npress q to close window!\n", vv.FieldByName("Reason").Interface().(string))
								}
							}
						}
						return nil
					})
				}()
			}
		} else {
			st.msg = fmt.Sprintf("error:%s\npress q to close window!\n", err.Error())
		}
		a.stack = append(a.stack, st)
	}
	return nil
}

func (a *app) drawStack(g *gocui.Gui) error {
	top := a.getStackTop()
	maxX, maxY := g.Size()
	for _, s := range a.stack {
		switch s.view {
		case "add":
			v, err := g.SetView(s.view, (maxX-editWidth)/2, 1, (maxX-editWidth)/2+editWidth, maxY-1)
			if err != nil {
				if err != gocui.ErrUnknownView {
					return err
				}

				if a.mainViews[a.activeMainView] == "sets" {
					v.Title = "addSet (editable)"
				} else {
					v.Title = "addNode (editable)"
				}

				v.Editable = true
				v.Wrap = true
			}

			if top == s {
				g.Cursor = true
			}
		default:
			v, err := g.SetView(s.view, (maxX-40)/2, 10, (maxX-40)/2+40, 20)
			if err != nil {
				if err != gocui.ErrUnknownView {
					return err
				}
				v.Wrap = true

				if err := g.SetKeybinding(s.view, 'q', gocui.ModNone, a.onQ); err != nil {
					log.Panicln(err)
				}

				if err := g.SetKeybinding(s.view, gocui.KeyEnter, gocui.ModNone, a.onEnter); err != nil {
					log.Panicln(err)
				}

			}
			v.Clear()
			fmt.Fprintf(v, s.msg)
		}
	}

	if top != nil {
		if _, err := setCurrentViewOnTop(g, top.view); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	//首先显示主视图
	vSets, err := g.SetView("sets", 0, 0, setsWidth, maxY-1)
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
	for _, s := range a.sets {
		fmt.Fprintf(vSets, "set:%d\n", s.Id)
	}

	vNode, err := g.SetView("nodes", setsWidth, 0, maxX-1, maxY-1)
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
	}
	vNode.Clear()

	if i, s := a.getSelectedSet(); s != nil {
		vSets.SetCursor(0, i)
		for _, v := range s.Nodes {
			fmt.Fprintf(vNode, "node:%d host:%s servicePort:%d raftPort:%d\n", v.Id, v.Host, v.ServicePort, v.RaftPort)
		}

		if i, n := a.getSelectedNode(); n != nil {
			vNode.SetCursor(0, i)
		} else {
			vNode.SetCursor(0, 0)
		}

	} else {
		vSets.SetCursor(0, 0)
		vNode.SetCursor(0, 0)
	}

	g.Cursor = false

	if len(a.stack) > 0 {
		if err = a.drawStack(g); err != nil {
			return err
		}
	} else {
		if _, err = setCurrentViewOnTop(g, a.mainViews[a.activeMainView]); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) changeMainView(g *gocui.Gui, v *gocui.View) error {
	if a.getStackTop() == nil {
		a.activeMainView = (a.activeMainView + 1) % len(a.mainViews)
	}
	return nil
}

func (a *app) cursorMovement(d int) func(g *gocui.Gui, v *gocui.View) error {
	return func(g *gocui.Gui, v *gocui.View) error {
		if nil != a.getStackTop() {
			return nil
		}

		if i, s := a.getSelectedSet(); nil != s {
			switch a.mainViews[a.activeMainView] {
			case "sets":
				next := i + d
				if next >= 0 && next < len(a.sets) {
					a.setSelected = a.sets[next].Id
					a.nodeSelected = 0
					if len(a.sets[next].Nodes) > 0 {
						a.nodeSelected = a.sets[next].Nodes[0].Id
					}
				}
			case "nodes":
				if i, n := a.getSelectedNode(); nil != n {
					next := i + d
					if next >= 0 && next < len(s.Nodes) {
						a.nodeSelected = s.Nodes[next].Id
					}
				}
			default:

			}
		}
		return nil
	}
}

func (a *app) getDepolyment(cli *consoleHttp.Client, g *gocui.Gui) {
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
			a.sets = resp.Sets
			return nil
		})
	}
}

func (a *app) onEnter(g *gocui.Gui, v *gocui.View) error {
	if top := a.getStackTop(); nil != top && nil != top.onEnter {
		top.onEnter()
	}
	return nil
}

func (a *app) onQ(g *gocui.Gui, v *gocui.View) error {
	if top := a.getStackTop(); nil != top && nil != top.onQ {
		top.onQ()
	}
	return nil
}

func (a *app) onToggleAdd(g *gocui.Gui, v *gocui.View) error {
	if top := a.getStackTop(); nil == top {
		a.stack = append(a.stack, &stackItem{view: "add"})
	} else if top.view == "add" {
		a.clearStack(g)
	}
	return nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

func main() {

	l := logger.NewZapLogger("depmntcli.log", "./log", "info", 1024*1024, 14, 14, false)

	logger.InitLogger(l)

	logger.GetSugar().Infof("start")

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

	g.Highlight = true
	g.Cursor = false
	g.SelFgColor = gocui.ColorGreen

	httpcli := consoleHttp.NewClient(*pdservice)

	a := app{
		mainViews: []string{"sets", "nodes"},
		httpcli:   httpcli,
	}

	g.SetManagerFunc(a.layout)

	go a.getDepolyment(httpcli, g)

	go func() {
		for {
			select {
			case <-time.After(1000 * time.Millisecond):
				a.getDepolyment(httpcli, g)
			}
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyTab, gocui.ModNone, a.changeMainView); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyArrowUp, gocui.ModNone, a.cursorMovement(-1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyArrowDown, gocui.ModNone, a.cursorMovement(1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlA, gocui.ModNone, a.onToggleAdd); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlO, gocui.ModNone, a.onCtrlSpace); err != nil {
		log.Panicln(err)
	}

	//if err := g.SetKeybinding("", gocui.KeyCtrlR, gocui.ModNone, onToggleRemove); err != nil {
	//	log.Panicln(err)
	//}

	/*if err := g.SetKeybinding("", gocui.KeyCtrlH, gocui.ModNone, onToggleHelp); err != nil {
		log.Panicln(err)
	}*/

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
}
