package main

import (
	"flag"
	//"fmt"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/logger"
	//"github.com/sniperHW/flyfish/server/app/cli/ui"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	"log"
	//"net/http"
	//_ "net/http/pprof"
	"sync"
	"time"
)

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

type sceneI interface {
	onTimeout(*gocui.Gui, *consoleHttp.Client)
	canChangeView() bool
	clear(*gocui.Gui)
	onActive(*gocui.Gui, *consoleHttp.Client)
	Layout(*gocui.Gui) error
	help(*gocui.Gui)
}

func main() {

	l := logger.NewZapLogger("listkv.log", "./log", "info", 1024*1024, 14, 14, false)

	logger.InitLogger(l)

	logger.GetSugar().Infof("begin")

	flag.Parse()

	pdservice := flag.String("pdservice", "localhost:8111", "ip1:port1;ip2:port2;...")

	g, err := gocui.NewGui(gocui.Output256)
	if err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	httpcli := consoleHttp.NewClient(*pdservice)

	var mtx sync.Mutex
	var actived int
	scenes := []sceneI{newListKv(g), newDepmnt(g, httpcli), newMeta(g, httpcli)}

	scenes[actived].onActive(g, httpcli)

	g.SelFgColor = gocui.ColorGreen
	g.Highlight = true
	g.InputEsc = true
	g.SetManagerFunc(func(g *gocui.Gui) error {
		scenes[actived].Layout(g)
		return nil
	})

	go func() {
		for {
			select {
			case <-time.After(1000 * time.Millisecond):
				mtx.Lock()
				scenes[actived].onTimeout(g, httpcli)
				mtx.Unlock()
			}
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlN, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		if len(scenes) > 0 {
			mtx.Lock()
			if scenes[actived].canChangeView() {
				scenes[actived].clear(g)
				actived = (actived + 1) % len(scenes)
				scenes[actived].onActive(g, httpcli)
			}
			mtx.Unlock()
		}
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	if err := g.SetKeybinding("", gocui.KeyCtrlH, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		if len(scenes) > 0 {
			scenes[actived].help(g)
		}
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}

}
