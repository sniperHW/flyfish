package main

import (
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
)

func makeHelp(g *gocui.Gui, scene *ui.Scene, msg string) {
	maxX, maxY := g.Size()

	var (
		width  = maxX / 8
		height = maxY / 8
	)

	layer := &ui.Layer{
		Name: "help",
	}

	helpView := &ui.View{
		Name:  "help",
		Title: "Help",
		Option: ui.UIOption{
			Wrap:      true,
			Highlight: true,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, msg)
		},
	}

	helpView.Option.LeftTopX = (maxX / 2) - width
	helpView.Option.LeftTopY = (maxY / 2) - height
	helpView.Option.RightBottomX = (maxX / 2) + width
	helpView.Option.RightBottomY = (maxY / 2) + height

	btnOk := &ui.View{
		Name:       "help-ok",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			Z:            1,
			LeftTopX:     maxX/2 - 4,
			LeftTopY:     helpView.Option.RightBottomY - 3,
			RightBottomX: maxX/2 + 4,
			RightBottomY: helpView.Option.RightBottomY - 1,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintln(v, "Ok")
		},
	}

	btnOk.AddKey(gocui.KeyEnter, func(g *gocui.Gui, _ *gocui.View) error {
		scene.Pop(g)
		return nil
	})

	layer.AddView(helpView)
	layer.AddView(btnOk)
	scene.Push(layer)

}
