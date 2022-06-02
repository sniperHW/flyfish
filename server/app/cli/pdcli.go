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
	help(*gocui.Gui, string)
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

	sceneMovement := func(d int) func(_ *gocui.Gui, _ *gocui.View) error {
		return func(gui *gocui.Gui, v *gocui.View) error {
			if len(scenes) > 0 {
				mtx.Lock()
				next := actived + d
				if next < 0 {
					next = 0
				} else if next >= len(scenes)-1 {
					next = len(scenes) - 1
				}

				if next != actived {
					scenes[actived].clear(g)
					actived = next
					scenes[actived].onActive(g, httpcli)
				}
				mtx.Unlock()
			}
			return nil
		}
	}

	arrowBind := false
	g.SetManagerFunc(func(g *gocui.Gui) error {
		if scenes[actived].canChangeView() {
			if !arrowBind {
				arrowBind = true
				if err := g.SetKeybinding("", gocui.KeyArrowLeft, gocui.ModNone, sceneMovement(-1)); err != nil {
					log.Panicln(err)
				}

				if err := g.SetKeybinding("", gocui.KeyArrowRight, gocui.ModNone, sceneMovement(1)); err != nil {
					log.Panicln(err)
				}
			}
		} else {
			if arrowBind {
				arrowBind = false
				g.DeleteKeybinding("", gocui.KeyArrowLeft, gocui.ModNone)
				g.DeleteKeybinding("", gocui.KeyArrowRight, gocui.ModNone)
			}
		}
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

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	helpMsg := "<-:previous scene\n->: next scene\nTab: change view\n"

	if err := g.SetKeybinding("", gocui.KeyCtrlH, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		if len(scenes) > 0 {
			scenes[actived].help(g, helpMsg)
		}
		return nil
	}); err != nil {
		log.Panicln(err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}

}
