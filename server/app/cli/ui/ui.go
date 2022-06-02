package ui

import (
	"github.com/jroimartin/gocui"
	"sort"
)

func setCurrentViewOnTop(g *gocui.Gui, name string) (*gocui.View, error) {
	if _, err := g.SetCurrentView(name); err != nil {
		return nil, err
	}
	return g.SetViewOnTop(name)
}

type stack struct {
	data []*Layer
}

func (s *stack) Bottom() *Layer {
	if len(s.data) > 0 {
		return s.data[0]
	} else {
		return nil
	}
}

func (s *stack) Top() *Layer {
	if len(s.data) > 0 {
		return s.data[len(s.data)-1]
	} else {
		return nil
	}
}

func (s *stack) Pop(g *gocui.Gui) {
	if top := s.Top(); nil != top {
		top.Clear(g)
		s.data[len(s.data)-1] = nil
		s.data = s.data[:len(s.data)-1]
	}
}

func (s *stack) Clear(g *gocui.Gui) {
	for len(s.data) > 0 {
		s.Pop(g)
	}
}

func (s *stack) Push(l *Layer) {
	s.data = append(s.data, l)
}

func (s *stack) Len() int {
	return len(s.data)
}

type UIOption struct {
	LeftTopX     int
	LeftTopY     int
	RightBottomX int
	RightBottomY int
	Editable     bool
	Wrap         bool
	Cursor       bool
	Highlight    bool
	SelBgColor   gocui.Attribute
	SelFgColor   gocui.Attribute
	Z            int
}

type View struct {
	Name         string
	Title        string
	Option       UIOption
	SelectAble   bool
	keys         map[interface{}]func(*gocui.Gui, *gocui.View) error
	OutPut       func(*gocui.View)
	OnViewCreate func(*gocui.Gui, *gocui.View)
	Ctx          interface{}
}

func (v *View) AddKey(k interface{}, fn func(*gocui.Gui, *gocui.View) error) {
	if nil == v.keys {
		v.keys = map[interface{}]func(*gocui.Gui, *gocui.View) error{}
	}
	v.keys[k] = fn
}

func (v *View) Layout(gui *gocui.Gui) {
	vv, err := gui.SetView(v.Name, v.Option.LeftTopX, v.Option.LeftTopY, v.Option.RightBottomX, v.Option.RightBottomY)
	if err != nil {
		if err != gocui.ErrUnknownView {
			panic(err)
		}

		vv.Title = v.Title
		vv.Editable = v.Option.Editable
		vv.Wrap = v.Option.Wrap
		vv.Highlight = v.Option.Highlight
		vv.SelBgColor = v.Option.SelBgColor
		vv.SelFgColor = v.Option.SelFgColor
		v.bindkey(gui)

		if nil != v.OnViewCreate {
			v.OnViewCreate(gui, vv)
		}
	}

	if nil != v.OutPut {
		v.OutPut(vv)
	}
}

func (v *View) Clear(gui *gocui.Gui) {
	gui.DeleteView(v.Name)
	gui.DeleteKeybindings(v.Name)
}

func (v *View) bindkey(gui *gocui.Gui) {
	for k, _ := range v.keys {
		key := k
		if err := gui.SetKeybinding(v.Name, key, gocui.ModNone, func(g *gocui.Gui, vv *gocui.View) error {
			if fn, ok := v.keys[key]; ok {
				fn(g, vv)
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}
}

type Layer struct {
	Name         string
	Ctx          interface{}
	BeforeLayout func(*gocui.Gui)
	AfterLayout  func(*gocui.Gui)
	views        map[string]*View
	selectable   []*View
	actived      int
}

func (l *Layer) DeleteView(gui *gocui.Gui, name string) bool {
	if v, ok := l.views[name]; ok {
		delete(l.views, name)
		v.Clear(gui)
		i := 0
		for ; i < len(l.selectable); i++ {
			if l.selectable[i].Name == name {
				l.selectable[i] = nil
				break
			}
		}
		if i != len(l.selectable) {
			for ; i < len(l.selectable)-1; i++ {
				l.selectable[i] = l.selectable[i+1]
			}
			l.selectable = l.selectable[:len(l.selectable)-1]
			if l.actived > len(l.selectable) {
				l.actived = 0
			}
		}
		return true
	} else {
		return false
	}
}

func (l *Layer) AddView(v *View) {
	if nil == l.views {
		l.views = map[string]*View{}
	}
	if _, ok := l.views[v.Name]; !ok {
		l.views[v.Name] = v
		if v.SelectAble {
			l.selectable = append(l.selectable, v)
		}
	}
}

func (l *Layer) GetViewByName(n string) *View {
	return l.views[n]
}

func (l *Layer) GetActiveView() *View {
	if len(l.selectable) > 0 {
		return l.selectable[l.actived]
	}
	return nil
}

func (l *Layer) SetActiveByName(n string) bool {
	for k, v := range l.selectable {
		if v.Name == n {
			l.actived = k
			return true
		}
	}
	return false
}

func (l *Layer) ChangeActive() {
	l.actived = (l.actived + 1) % len(l.selectable)
}

func (l *Layer) Layout(g *gocui.Gui) {

	if l.BeforeLayout != nil {
		l.BeforeLayout(g)
	}

	views := make([]*View, 0, len(l.views))
	for _, v := range l.views {
		views = append(views, v)
	}

	sort.Slice(views, func(l, r int) bool {
		return views[l].Option.Z < views[r].Option.Z
	})

	for _, v := range views {
		v.Layout(g)
	}

	if l.AfterLayout != nil {
		l.AfterLayout(g)
	}
}

func (l *Layer) Clear(g *gocui.Gui) {
	for _, v := range l.views {
		v.Clear(g)
	}
}

type Scene struct {
	stack
	Name string
}

func (s *Scene) Layout(g *gocui.Gui) error {
	g.Cursor = false

	for _, v := range s.data {
		v.Layout(g)
	}

	if top := s.Top(); top != nil {
		if actived := top.GetActiveView(); actived != nil {
			setCurrentViewOnTop(g, actived.Name)
			if actived.Option.Cursor {
				g.Cursor = true
			}
		}
	}

	return nil
}

func (s *Scene) Clear(g *gocui.Gui) {
	for _, v := range s.data {
		v.Clear(g)
	}
}
