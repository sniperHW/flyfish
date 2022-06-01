package main

import (
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
)

func makeEdit(g *gocui.Gui, scene *ui.Scene, name string, hint string, onOk func(editView *gocui.View)) {

	if nil == onOk {
		return
	}

	layer := &ui.Layer{
		Name: fmt.Sprintf("editlayer-%s", name),
	}

	btnOk := &ui.View{
		Name:       fmt.Sprintf("edit-%s-ok", name),
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, "Ok")
		},
	}

	btnCancel := &ui.View{
		Name:       fmt.Sprintf("edit-%s-cancel", name),
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, "Cancel")
		},
	}

	boundView := &ui.View{
		Name:  fmt.Sprintf("edit-%s", name),
		Title: name,
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, hint)
		},
	}

	editArea := &ui.View{
		Name:       fmt.Sprintf("edit-editarea-%s", name),
		SelectAble: true,
		Option: ui.UIOption{
			Cursor:    true,
			Wrap:      true,
			Highlight: true,
			Editable:  true,
			Z:         1,
		},
	}

	maxX, _ := g.Size()

	boundView.Option.LeftTopX = (maxX - 60) / 2
	boundView.Option.LeftTopY = 5
	boundView.Option.RightBottomX = (maxX-60)/2 + 60

	editArea.Option.LeftTopX = (maxX - 58) / 2
	editArea.Option.LeftTopY = boundView.Option.LeftTopY + 4
	editArea.Option.RightBottomX = (maxX-58)/2 + 58
	editArea.Option.RightBottomY = editArea.Option.LeftTopY + 10

	boundView.Option.RightBottomY = editArea.Option.RightBottomY + 5

	btnOk.Option.LeftTopX = boundView.Option.LeftTopX + 5
	btnOk.Option.LeftTopY = editArea.Option.RightBottomY + 1
	btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + 7
	btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2

	btnCancel.Option.LeftTopX = boundView.Option.RightBottomX - 12
	btnCancel.Option.LeftTopY = editArea.Option.RightBottomY + 1
	btnCancel.Option.RightBottomX = btnCancel.Option.LeftTopX + 7
	btnCancel.Option.RightBottomY = btnCancel.Option.LeftTopY + 2

	editArea.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnOk.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnOk.AddKey(gocui.KeyEnter, func(g *gocui.Gui, _ *gocui.View) error {
		if editView, _ := g.View(editArea.Name); nil != editView {
			onOk(editView)
		}
		return nil
	})

	btnCancel.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		layer.ChangeActive()
		return nil
	})

	btnCancel.AddKey(gocui.KeyEnter, func(g *gocui.Gui, vv *gocui.View) error {
		scene.Pop(g)
		return nil
	})

	layer.AddView(boundView)
	layer.AddView(editArea)
	layer.AddView(btnOk)
	layer.AddView(btnCancel)
	layer.SetActiveByName(editArea.Name)

	scene.Push(layer)
}
