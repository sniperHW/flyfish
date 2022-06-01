package main

import (
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
)

func makeMsg(g *gocui.Gui, scene *ui.Scene, name string, msg string, onOk func(), onCancel func()) {
	layer := &ui.Layer{
		Name: fmt.Sprintf("msg-%s", name),
	}

	var btnOk *ui.View
	var btnCancel *ui.View

	boundView := &ui.View{
		Name:  fmt.Sprintf("msg-%s", name),
		Title: "message",
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, msg)
		},
	}

	maxX, _ := g.Size()
	boundView.Option.LeftTopX = (maxX - 60) / 2
	boundView.Option.LeftTopY = 5
	boundView.Option.RightBottomX = (maxX-60)/2 + 60
	boundView.Option.RightBottomY = 15

	if nil != onOk {
		btnOk = &ui.View{
			Name:       fmt.Sprintf("msg-%s-ok", name),
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

		btnOk.AddKey(gocui.KeyEnter, func(g *gocui.Gui, _ *gocui.View) error {
			onOk()
			return nil
		})
	}

	if nil != onCancel {
		btnCancel = &ui.View{
			Name:       fmt.Sprintf("msg-%s-cancel", name),
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

		btnCancel.AddKey(gocui.KeyEnter, func(g *gocui.Gui, _ *gocui.View) error {
			onCancel()
			return nil
		})
	}

	if nil != onOk && nil != onCancel {
		btnOk.Option.LeftTopX = boundView.Option.LeftTopX + 5
		btnOk.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + 5
		btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2

		btnCancel.Option.LeftTopX = boundView.Option.RightBottomX - 10
		btnCancel.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnCancel.Option.RightBottomX = btnCancel.Option.LeftTopX + 5
		btnCancel.Option.RightBottomY = btnCancel.Option.LeftTopY + 2

		btnCancel.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
			layer.ChangeActive()
			return nil
		})

		btnOk.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
			layer.ChangeActive()
			return nil
		})

	} else if nil != onOk {
		btnOk.Option.LeftTopX = boundView.Option.LeftTopX + 5
		btnOk.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + 5
		btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2
	} else if nil != onCancel {
		btnCancel.Option.LeftTopX = boundView.Option.RightBottomX - 10
		btnCancel.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnCancel.Option.RightBottomX = btnCancel.Option.LeftTopX + 5
		btnCancel.Option.RightBottomY = btnCancel.Option.LeftTopY + 2
	}

	if nil != btnOk {
		layer.AddView(btnOk)
	}

	if nil != btnCancel {
		layer.AddView(btnCancel)
	}

	layer.AddView(boundView)
	scene.Push(layer)
}
