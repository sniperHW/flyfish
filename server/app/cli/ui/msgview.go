package ui

import (
	"fmt"
	"github.com/jroimartin/gocui"
)

func MakeMsg(g *gocui.Gui, scene *Scene, name string, msg string, onOk func(), onCancel func()) {

	maxX, maxY := g.Size()

	var (
		width   = maxX / 8
		height  = maxY / 10
		btWidth = 8
	)

	layer := &Layer{
		Name: fmt.Sprintf("msg-%s", name),
	}

	var btnOk *View
	var btnCancel *View

	boundView := &View{
		Name:  fmt.Sprintf("msg-%s", name),
		Title: "message",
		Option: UIOption{
			Wrap:      true,
			Highlight: true,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, msg)
		},
	}

	boundView.Option.LeftTopX = (maxX / 2) - width
	boundView.Option.LeftTopY = (maxY / 2) - height
	boundView.Option.RightBottomX = (maxX / 2) + width
	boundView.Option.RightBottomY = (maxY / 2) + height

	if nil != onOk {
		btnOk = &View{
			Name:       fmt.Sprintf("msg-%s-ok", name),
			SelectAble: true,
			Option: UIOption{
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
		btnCancel = &View{
			Name:       fmt.Sprintf("msg-%s-cancel", name),
			SelectAble: true,
			Option: UIOption{
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
		btnOk.Option.LeftTopX = maxX/2 - 2 - btWidth
		btnOk.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + btWidth
		btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2

		btnCancel.Option.LeftTopX = maxX/2 + 2
		btnCancel.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnCancel.Option.RightBottomX = btnCancel.Option.LeftTopX + btWidth
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
		btnOk.Option.LeftTopX = maxX/2 - btWidth/2
		btnOk.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnOk.Option.RightBottomX = btnOk.Option.LeftTopX + btWidth
		btnOk.Option.RightBottomY = btnOk.Option.LeftTopY + 2
	} else if nil != onCancel {
		btnCancel.Option.LeftTopX = maxX/2 - btWidth/2
		btnCancel.Option.LeftTopY = boundView.Option.RightBottomY - 5
		btnCancel.Option.RightBottomX = btnOk.Option.LeftTopX + btWidth
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
