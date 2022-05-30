package main

import (
	"flag"
	"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

type view interface {
	name() string
	layout(*gocui.Gui)
	clear(*gocui.Gui)
	onActive(*gocui.Gui, *consoleHttp.Client)
	onTimeout(*gocui.Gui, *consoleHttp.Client)
	canChangeView() bool
	keyBinding(*gocui.Gui)
	deleteKeyBinding(*gocui.Gui)
}

func setCurrentViewOnTop(g *gocui.Gui, name string) (*gocui.View, error) {
	if _, err := g.SetCurrentView(name); err != nil {
		return nil, err
	}
	return g.SetViewOnTop(name)
}

type app struct {
	sync.Mutex
	actived  int
	views    []view
	showHelp bool
}

const (
	helpWidth = 80
)

func (a *app) layout(g *gocui.Gui) error {
	a.views[int(a.actived)].layout(g)
	if a.showHelp {
		maxX, maxY := g.Size()
		v, err := g.SetView("help", (maxX-helpWidth)/2, 1, (maxX-helpWidth)/2+helpWidth, maxY-1)
		if err != nil {
			if err != gocui.ErrUnknownView {
				return err
			}

			v.Title = "help"
			v.Wrap = true
		}

		g.Highlight = true
		g.Cursor = false
		g.SelFgColor = gocui.ColorGreen

		v.Clear()

		fmt.Fprintln(v, "ctrl + N: change to next scene")
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
	return nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

func main() {

	l := logger.NewZapLogger("listkv.log", "./log", "info", 1024*1024, 14, 14, false)

	logger.InitLogger(l)

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

	httpcli := consoleHttp.NewClient(*pdservice)

	app := &app{}

	app.views = append(app.views, &listkv{})
	app.views = append(app.views, &depmnt{
		httpcli:   httpcli,
		mainViews: []string{"depmnt-sets", "depmnt-nodes"},
	})

	g.InputEsc = true
	g.SetManagerFunc(app.layout)

	app.views[app.actived].onActive(g, httpcli)

	go func() {
		for {
			select {
			case <-time.After(1000 * time.Millisecond):
				app.Lock()
				app.views[app.actived].onTimeout(g, httpcli)
				app.Unlock()
			}
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlN, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		if !app.showHelp && len(app.views) > 0 {
			app.Lock()
			if app.views[app.actived].canChangeView() {
				app.views[app.actived].clear(g)
				app.actived = (app.actived + 1) % len(app.views)
				app.views[app.actived].onActive(g, httpcli)
			}
			app.Unlock()
		}
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlH, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		app.showHelp = !app.showHelp
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyEsc, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		app.showHelp = false
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
}
