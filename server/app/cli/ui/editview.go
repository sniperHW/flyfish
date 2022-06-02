package ui

import (
	"fmt"
	"github.com/jroimartin/gocui"
)

func MakeEdit(g *gocui.Gui, scene *Scene, name string, hint string, onOk func(editView *gocui.View)) {

	if nil == onOk {
		return
	}

	maxX, maxY := g.Size()

	var (
		width   = maxX / 6
		height  = maxY / 4
		btWidth = 8
	)

	layer := &Layer{
		Name: fmt.Sprintf("editlayer-%s", name),
	}

	btnOk := &View{
		Name:       fmt.Sprintf("edit-%s-ok", name),
		SelectAble: true,
		Option: UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, CenterPrint(btWidth, "Ok"))
		},
	}

	btnCancel := &View{
		Name:       fmt.Sprintf("edit-%s-cancel", name),
		SelectAble: true,
		Option: UIOption{
			Wrap:      true,
			Highlight: true,
			Z:         2,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, CenterPrint(btWidth, "Cancel"))
		},
	}

	boundView := &View{
		Name:  fmt.Sprintf("edit-%s", name),
		Title: CenterPrint(width, name),
		Option: UIOption{
			Wrap:      true,
			Highlight: true,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, hint)
		},
	}

	editArea := &View{
		Name:       fmt.Sprintf("edit-editarea-%s", name),
		SelectAble: true,
		Option: UIOption{
			Cursor:    true,
			Wrap:      true,
			Highlight: true,
			Editable:  true,
			Z:         1,
		},
	}

	boundView.Option.LeftTopX = (maxX / 2) - width
	boundView.Option.LeftTopY = (maxY / 2) - height
	boundView.Option.RightBottomX = (maxX / 2) + width
	boundView.Option.RightBottomY = (maxY / 2) + height

	editArea.Option.LeftTopX = (maxX / 2) - width + 1
	editArea.Option.LeftTopY = boundView.Option.LeftTopY + 5
	editArea.Option.RightBottomX = (maxX / 2) + width - 1
	editArea.Option.RightBottomY = editArea.Option.LeftTopY + 10

	btnOk.Option.LeftTopX = maxX/2 - 2 - btWidth
	btnOk.Option.LeftTopY = editArea.Option.RightBottomY + 2
	btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + btWidth
	btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2

	btnCancel.Option.LeftTopX = maxX/2 + 2
	btnCancel.Option.LeftTopY = editArea.Option.RightBottomY + 2
	btnCancel.Option.RightBottomX = btnCancel.Option.LeftTopX + btWidth
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
