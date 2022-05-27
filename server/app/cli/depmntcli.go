package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strconv"
	"strings"
	"time"
)

type hintWin struct {
	wait bool
	msg  string
	fn   func()
}

var (
	setSelected     int32
	nodeSelected    int32
	sets            []*sproto.Set
	httpcli         *consoleHttp.Client
	currentMainView string = "sets"
	showAdd         bool
	showHelp        bool
	hint            *hintWin
)

func getSelectedSet() (int, *sproto.Set) {
	for i, v := range sets {
		if setSelected == v.Id {
			return i, v
		}
	}

	if len(sets) > 0 {
		return 0, sets[0]
	} else {
		return -1, nil
	}
}

func getSelectedNode() (int, *sproto.Node) {
	if _, s := getSelectedSet(); s != nil {
		for i, v := range s.Nodes {
			if nodeSelected == v.Id {
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

const (
	setsWidth = 15 //sets控件的最大宽度
	editWidth = 80
)

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

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
	for _, s := range sets {
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

	if i, s := getSelectedSet(); s != nil {
		vSets.SetCursor(0, i)
		for _, v := range s.Nodes {
			fmt.Fprintf(vNode, "node:%d host:%s servicePort:%d raftPort:%d\n", v.Id, v.Host, v.ServicePort, v.RaftPort)
		}

		if i, n := getSelectedNode(); n != nil {
			vNode.SetCursor(0, i)
		} else {
			vNode.SetCursor(0, 0)
		}

	} else {
		vSets.SetCursor(0, 0)
		vNode.SetCursor(0, 0)
	}

	g.Cursor = false

	if nil != hint {
		v, err := g.SetView("hint", (maxX-40)/2, 10, (maxX-40)/2+40, 20)
		if err != nil {
			if err != gocui.ErrUnknownView {
				return err
			}
			v.Wrap = true
		}

		v.Clear()

		if hint.wait {
			fmt.Fprintln(v, "Waitting response from server...")
		} else {
			fmt.Fprintf(v, "%s (press enter to close)", hint.msg)
		}

		if _, err = setCurrentViewOnTop(g, "hint"); err != nil {
			return err
		}

	} else {

		g.DeleteView("hint")
		if showAdd {
			v, err := g.SetView("add", (maxX-editWidth)/2, 1, (maxX-editWidth)/2+editWidth, maxY-1)
			if err != nil {
				if err != gocui.ErrUnknownView {
					return err
				}

				if currentMainView == "sets" {
					v.Title = "addSet (editable)"
				} else {
					v.Title = "addNode (editable)"
				}

				v.Editable = true
				v.Wrap = true
			}

			g.Cursor = true

			if _, err = setCurrentViewOnTop(g, "add"); err != nil {
				return err
			}
		} else {
			g.DeleteView("add")
		}

		if showHelp {
			v, err := g.SetView("help", (maxX-editWidth)/2, 1, (maxX-editWidth)/2+editWidth, maxY-1)
			if err != nil {
				if err != gocui.ErrUnknownView {
					return err
				}

				v.Title = "help"
				v.Wrap = true
			}

			g.Cursor = false

			v.Clear()

			fmt.Fprintln(v, "tab: change view")
			fmt.Fprintln(v, "ctrl + A: Add Set/Node (Depending on the current view)")
			fmt.Fprintln(v, "ctrl + O: Confirm Add Set/Node")
			fmt.Fprintln(v, "ctrl + R: Remove the selected Set/Node (Depending on the current view)")
			fmt.Fprintln(v, "ctrl + M: Mark Clear the selected Set")

			if _, err = setCurrentViewOnTop(g, "help"); err != nil {
				return err
			}

		} else {
			g.DeleteView("help")
		}

		if !showAdd && !showHelp {
			setCurrentViewOnTop(g, currentMainView)
		}
	}
	return nil
}

func getDepolyment(cli *consoleHttp.Client, g *gocui.Gui) {
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
			sets = resp.Sets
			return nil
		})
	}
}

func changeMainView(g *gocui.Gui, v *gocui.View) error {
	if !showAdd && !showHelp && hint == nil {
		if currentMainView == "sets" {
			currentMainView = "nodes"
		} else {
			currentMainView = "sets"
		}
	}
	return nil
}

func cursorMovement(d int) func(g *gocui.Gui, v *gocui.View) error {
	return func(g *gocui.Gui, v *gocui.View) error {
		if i, s := getSelectedSet(); nil != s {
			if currentMainView == "sets" {
				next := i + d
				if next >= 0 && next < len(sets) {
					setSelected = sets[next].Id
					nodeSelected = 0
					if len(sets[next].Nodes) > 0 {
						nodeSelected = sets[next].Nodes[0].Id
					}
				}
			} else if currentMainView == "nodes" {
				if i, n := getSelectedNode(); nil != n {
					next := i + d
					if next >= 0 && next < len(s.Nodes) {
						nodeSelected = s.Nodes[next].Id
					}
				}
			}
		}
		return nil
	}
}

func onToggleAdd(g *gocui.Gui, v *gocui.View) error {
	showAdd = !showAdd
	return nil
}

func onToggleHelp(g *gocui.Gui, v *gocui.View) error {
	showHelp = !showHelp
	return nil
}

func makeRemNodeReq(v *gocui.View) (*sproto.RemNode, error) {
	if _, s := getSelectedSet(); s == nil {
		return nil, errors.New("no set selected")
	} else {
		if _, n := getSelectedNode(); n == nil {
			return nil, errors.New("no node selected")
		} else {
			return &sproto.RemNode{
				SetID:  s.Id,
				NodeID: n.Id,
			}, nil
		}
	}
}

func makeRemSetReq(v *gocui.View) (*sproto.RemSet, error) {
	if _, s := getSelectedSet(); s == nil {
		return nil, errors.New("no set selected")
	} else {
		return &sproto.RemSet{
			SetID: s.Id,
		}, nil
	}
}

func onToggleRemove(g *gocui.Gui, v *gocui.View) error {
	if currentMainView == "sets" {
		hint = &hintWin{}
		if req, err := makeRemSetReq(v); err == nil {
			hint.msg = fmt.Sprintf("do you really want to remove set:%d? press enter to Confirm!", req.SetID)
			hint.fn = func() {
				go func() {
					resp, err := httpcli.Call(req, &sproto.RemSetResp{})
					g.Update(func(g *gocui.Gui) error {
						hint.wait = false
						if nil != err {
							hint.msg = err.Error()
						} else {
							if resp.(*sproto.RemSetResp).Ok {
								hint.msg = "Ok"
							} else {
								hint.msg = resp.(*sproto.RemSetResp).Reason
							}
						}
						return nil
					})
				}()
			}
		} else {
			hint.msg = err.Error()
		}
	} else if currentMainView == "nodes" {
		hint = &hintWin{}
		if req, err := makeRemNodeReq(v); err == nil {
			hint.msg = fmt.Sprintf("do you really want to remove node:%d? press enter to Confirm!", req.NodeID)
			hint.fn = func() {
				go func() {
					resp, err := httpcli.Call(req, &sproto.RemNodeResp{})
					g.Update(func(g *gocui.Gui) error {
						hint.wait = false
						if nil != err {
							hint.msg = err.Error()
						} else {
							if resp.(*sproto.RemNodeResp).Ok {
								hint.msg = "Ok"
							} else {
								hint.msg = resp.(*sproto.RemNodeResp).Reason
							}
						}
						return nil
					})
				}()
			}
		} else {
			hint.msg = err.Error()
		}
	}
	return nil
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

func makeAddSetReq(v *gocui.View) (*sproto.AddSet, error) {
	setID := int32(0)
	for i := 0; i < len(sets); i++ {
		if sets[i].Id-setID > int32(1) {
			break
		} else {
			setID = sets[i].Id
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

func makeAddNodeReq(v *gocui.View) (*sproto.AddNode, error) {
	if _, s := getSelectedSet(); s == nil {
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

func onToggleConfirm(g *gocui.Gui, v *gocui.View) error {
	if currentMainView == "sets" {
		hint = &hintWin{}
		if req, err := makeAddSetReq(v); err == nil {
			hint.msg = "press enter to Confirm!"
			hint.fn = func() {
				go func() {
					resp, err := httpcli.Call(req, &sproto.AddSetResp{})
					g.Update(func(g *gocui.Gui) error {
						hint.wait = false
						if nil != err {
							hint.msg = err.Error()
						} else {
							if resp.(*sproto.AddSetResp).Ok {
								hint.msg = "Ok"
							} else {
								hint.msg = resp.(*sproto.AddSetResp).Reason
							}
						}
						return nil
					})
				}()
			}
		} else {
			hint.msg = err.Error()
		}
	} else if currentMainView == "nodes" {
		hint = &hintWin{}
		if req, err := makeAddNodeReq(v); err == nil {
			hint.msg = "press enter to Confirm!"
			hint.fn = func() {
				go func() {
					resp, err := httpcli.Call(req, &sproto.AddNodeResp{})
					g.Update(func(g *gocui.Gui) error {
						hint.wait = false
						if nil != err {
							hint.msg = err.Error()
						} else {
							if resp.(*sproto.AddNodeResp).Ok {
								hint.msg = "Ok"
							} else {
								hint.msg = resp.(*sproto.AddNodeResp).Reason
							}
						}
						return nil
					})
				}()
			}
		} else {
			hint.msg = err.Error()
		}
	}
	return nil
}

func onToggleMarkClear(g *gocui.Gui, v *gocui.View) error {
	return nil
}

func onHintEnter(g *gocui.Gui, v *gocui.View) error {
	if nil == hint {
		return
	}

	if nil != hint.fn {
		hint.wait = true
		hint.fn()
		hint.fn = nil
	} else {
		hint = nil
		showAdd = false
	}
	return nil
}

func onOpCancel(g *gocui.Gui, v *gocui.View) error {
	hint = nil
	showAdd = false
	return nil
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

	httpcli = consoleHttp.NewClient(*pdservice)

	g.SetManagerFunc(layout)

	go getDepolyment(httpcli, g)

	go func() {
		for {
			select {
			case <-time.After(1000 * time.Millisecond):
				getDepolyment(httpcli, g)
			}
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyTab, gocui.ModNone, changeMainView); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.KeyArrowUp, gocui.ModNone, cursorMovement(-1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.KeyArrowDown, gocui.ModNone, cursorMovement(1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("nodes", gocui.KeyArrowUp, gocui.ModNone, cursorMovement(-1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("nodes", gocui.KeyArrowDown, gocui.ModNone, cursorMovement(1)); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.KeyCtrlA, gocui.ModNone, onToggleAdd); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.KeyCtrlR, gocui.ModNone, onToggleRemove); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("nodes", gocui.KeyCtrlA, gocui.ModNone, onToggleAdd); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("add", gocui.KeyCtrlA, gocui.ModNone, onToggleAdd); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("nodes", gocui.KeyCtrlR, gocui.ModNone, onToggleRemove); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlH, gocui.ModNone, onToggleHelp); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("add", gocui.KeyCtrlO, gocui.ModNone, onToggleConfirm); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("sets", gocui.KeyCtrlM, gocui.ModNone, onToggleMarkClear); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("hint", gocui.KeyEnter, gocui.ModNone, onHintEnter); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("hint", 'q', gocui.ModNone, onOpCancel); err != nil {
		log.Panicln(err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
}
