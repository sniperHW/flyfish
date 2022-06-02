package main

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jroimartin/gocui"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/server/app/cli/ui"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"reflect"
	"sort"
	//"strconv"
	"github.com/sniperHW/flyfish/logger"
	"strings"
	"sync/atomic"
)

type sceneMeta struct {
	scene         *ui.Scene
	httpcli       *consoleHttp.Client
	tableSelected string
	fieldSelected string
	meta          db.DbDef
	metaVersion   int64
}

func (sm *sceneMeta) getSelectedTable() (int, *db.TableDef) {
	for i, v := range sm.meta.TableDefs {
		if sm.tableSelected == v.Name {
			return i, v
		}
	}

	if len(sm.meta.TableDefs) > 0 {
		return 0, sm.meta.TableDefs[0]
	} else {
		return 0, nil
	}
}

func (sm *sceneMeta) getSelectedField() (int, *db.FieldDef) {
	if _, t := sm.getSelectedTable(); t != nil {
		for i, v := range t.Fields {
			if sm.fieldSelected == v.Name {
				return i, v
			}
		}

		if len(t.Fields) > 0 {
			return 0, t.Fields[0]
		}
	}
	return 0, nil
}

func (sm *sceneMeta) getMeta(cli *consoleHttp.Client, g *gocui.Gui) {
	if r, _ := cli.Call(&sproto.GetMeta{}, &sproto.GetMetaResp{}); r != nil {
		resp := r.(*sproto.GetMetaResp)
		if atomic.LoadInt64(&sm.metaVersion) == resp.Version {
			return
		}

		meta, err := db.MakeDbDefFromJsonString(resp.Meta)

		if nil != err {
			logger.GetSugar().Infof("%v", err.Error())
			return
		}

		sort.Slice(meta.TableDefs, func(l, r int) bool {
			return meta.TableDefs[l].Name < meta.TableDefs[r].Name
		})

		for _, v := range meta.TableDefs {
			sort.Slice(v.Fields, func(l, r int) bool {
				return v.Fields[l].Name < v.Fields[l].Name
			})
		}

		g.Update(func(g *gocui.Gui) error {
			atomic.StoreInt64(&sm.metaVersion, resp.Version)
			sm.meta = *meta
			return nil
		})
	}
}

func (sm *sceneMeta) onActive(g *gocui.Gui, httpcli *consoleHttp.Client) {
	g.Highlight = true
	g.Cursor = false
	sm.getMeta(httpcli, g)
	sm.createLayer(g)
}

func (sm *sceneMeta) onTimeout(g *gocui.Gui, httpcli *consoleHttp.Client) {
	sm.getMeta(httpcli, g)
}

func (sm *sceneMeta) canChangeView() bool {
	return sm.scene.Len() == 1
}

func (sm *sceneMeta) clear(g *gocui.Gui) {
	sm.scene.Pop(g)
}

func (sm *sceneMeta) Layout(g *gocui.Gui) error {
	return sm.scene.Layout(g)
}

func (sm *sceneMeta) makeAddReq(v *gocui.View) (req proto.Message, resp proto.Message, err error) {
	switch sm.scene.Bottom().GetActiveView().Name {
	case "meta-tables":
		req, err = sm.makeAddTableReq(v)
		resp = &sproto.MetaAddTableResp{}
	case "meta-fields":
		req, err = sm.makeAddFieldsReq(v)
		resp = &sproto.MetaAddFieldsResp{}
	}
	return
}

func (sm *sceneMeta) makeAddTableReq(v *gocui.View) (*sproto.MetaAddTable, error) {
	req := &sproto.MetaAddTable{
		Version: sm.metaVersion,
	}

	buffs := v.ViewBufferLines()
	if len(buffs) > 0 {
		if db.ContainsInvaildCharacter(buffs[0]) {
			return nil, errors.New("table name contains invaild character!")
		} else {
			req.Name = buffs[0]
		}
	} else {
		return nil, errors.New("missing table name!")
	}

	if len(buffs) > 1 {
		for i := 1; i < len(buffs); i++ {
			l := strings.Split(buffs[i], " ")
			if len(l) >= 2 {
				name := l[0]
				if db.ContainsInvaildCharacter(name) {
					return nil, errors.New("field name contains invaild character!")
				}
				tt := l[1]
				var defaultValue string
				if len(l) >= 3 {
					defaultValue = l[2]
				}
				req.Fields = append(req.Fields, &sproto.MetaFiled{
					Name:    name,
					Type:    tt,
					Default: defaultValue,
				})
			} else {
				return nil, errors.New("field (Name Type DefaultValue)!")
			}
		}
	} else {
		return nil, errors.New("missing fields!")
	}

	return req, nil
}

func (sm *sceneMeta) makeAddFieldsReq(v *gocui.View) (*sproto.MetaAddFields, error) {
	if _, t := sm.getSelectedTable(); t == nil {
		return nil, errors.New("no table selected")
	} else {
		buffs := v.ViewBufferLines()
		if len(buffs) == 0 {
			return nil, errors.New("missing fields!")
		}

		req := &sproto.MetaAddFields{
			Version: sm.metaVersion,
			Table:   t.Name,
		}

		for _, v := range buffs {
			l := strings.Split(v, " ")
			if len(l) >= 2 {
				name := l[0]
				if db.ContainsInvaildCharacter(name) {
					return nil, errors.New("field name contains invaild character!")
				}
				tt := l[1]
				var defaultValue string
				if len(l) >= 3 {
					defaultValue = l[2]
				}
				req.Fields = append(req.Fields, &sproto.MetaFiled{
					Name:    name,
					Type:    tt,
					Default: defaultValue,
				})
			} else {
				return nil, errors.New("field (Name Type DefaultValue)!")
			}

		}

		return req, nil
	}
}

func (sm *sceneMeta) createLayer(gui *gocui.Gui) {
	const (
		metaVersionHight = 2
		tableWidth       = 20 //sets控件的最大宽度
	)

	maxX, maxY := gui.Size()

	layer := &ui.Layer{
		Name: "meta",
	}

	cursorMovement := func(d int) func(_ *gocui.Gui, _ *gocui.View) error {
		return func(gui *gocui.Gui, v *gocui.View) error {
			logger.GetSugar().Infof(sm.scene.Bottom().GetActiveView().Name)
			switch sm.scene.Bottom().GetActiveView().Name {
			case "meta-tables":
				if i, t := sm.getSelectedTable(); nil != t {
					next := i + d
					if next >= 0 && next < len(sm.meta.TableDefs) {
						sm.tableSelected = sm.meta.TableDefs[next].Name
					}
				}
			case "meta-fields":
				if _, t := sm.getSelectedTable(); nil != t {
					if i, f := sm.getSelectedField(); nil != f {
						next := i + d
						if next >= 0 && next < len(t.Fields) {
							sm.fieldSelected = t.Fields[next].Name
						}
						logger.GetSugar().Info(sm.fieldSelected)
					}
				}
			}
			return nil
		}
	}

	viewMetaVersion := &ui.View{
		Name: "meta-version",
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			RightBottomX: maxX - 1,
			RightBottomY: metaVersionHight,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			fmt.Fprintf(v, "meta version:%d\n", sm.meta.Version)
		},
	}

	layer.AddView(viewMetaVersion)

	viewTables := &ui.View{
		Name:       "meta-tables",
		Title:      "Tables",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			SelBgColor:   gocui.ColorGreen,
			SelFgColor:   gocui.ColorBlack,
			LeftTopY:     metaVersionHight + 1,
			RightBottomX: tableWidth,
			RightBottomY: maxY - 1,
		},
		OutPut: func(v *gocui.View) {
			i, _ := sm.getSelectedTable()
			v.SetCursor(0, i)
			v.Clear()
			for _, t := range sm.meta.TableDefs {
				fmt.Fprintf(v, "%s version:%d\n", t.Name, t.Version)
			}
		},
		OnViewCreate: func(gui *gocui.Gui, v *gocui.View) {
			gui.SetCurrentView("meta-tables")
		},
	}

	layer.AddView(viewTables)

	viewTables.AddKey(gocui.KeyArrowUp, cursorMovement(-1))
	viewTables.AddKey(gocui.KeyArrowDown, cursorMovement(1))
	viewTables.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		if sm.scene.Bottom().Name == "meta" {
			layer.ChangeActive()
		}
		return nil
	})

	viewFields := &ui.View{
		Name:       "meta-fields",
		Title:      "Fields",
		SelectAble: true,
		Option: ui.UIOption{
			Wrap:         true,
			Highlight:    true,
			SelBgColor:   gocui.ColorGreen,
			SelFgColor:   gocui.ColorBlack,
			LeftTopX:     tableWidth + 1,
			LeftTopY:     metaVersionHight + 1,
			RightBottomX: maxX - 1,
			RightBottomY: maxY - 1,
		},
		OutPut: func(v *gocui.View) {
			v.Clear()
			if _, t := sm.getSelectedTable(); t != nil {
				for _, f := range t.Fields {
					fmt.Fprintf(v, "%s type:%s default:%s\n", f.Name, f.Type, f.DefaultValue)
				}
				i, _ := sm.getSelectedField()
				v.SetCursor(0, i)
			} else {
				v.SetCursor(0, 0)
			}
		},
	}

	layer.AddView(viewFields)

	viewFields.AddKey(gocui.KeyArrowUp, cursorMovement(-1))
	viewFields.AddKey(gocui.KeyArrowDown, cursorMovement(1))
	viewFields.AddKey(gocui.KeyTab, func(g *gocui.Gui, vv *gocui.View) error {
		if sm.scene.Bottom().Name == "meta" {
			layer.ChangeActive()
		}
		return nil
	})

	onCltA := func(_ *gocui.Gui, _ *gocui.View) error {
		var name string
		var hint string
		switch sm.scene.Bottom().GetActiveView().Name {
		case "meta-tables":
			name = "AddTable"
			hint = fmt.Sprintf("TableName\nFieldName Type DefaultValue\nFieldName Type DefaultValue\n... ")
		case "meta-fields":
			name = "AddFields"
			hint = fmt.Sprintf("FieldName Type DefaultValue\nFieldName Type DefaultValue\n... ")
		}

		ui.MakeEdit(gui, sm.scene, name, hint, func(editView *gocui.View) {
			req, resp, err := sm.makeAddReq(editView)
			if nil != err {
				ui.MakeMsg(gui, sm.scene, "meta-add-error", err.Error(), func() {
					sm.scene.Pop(gui)
				}, nil)
			} else {
				ui.MakeMsg(gui, sm.scene, "meta-add-wait-response", "Waitting response from server...", nil, nil)
				go func() {
					r, err := sm.httpcli.Call(req, resp)
					gui.Update(func(_ *gocui.Gui) error {
						sm.scene.Pop(gui)
						if nil != err {
							ui.MakeMsg(gui, sm.scene, "meta-add-error", err.Error(), func() {
								sm.scene.Pop(gui)
							}, nil)
						} else {
							vv := reflect.ValueOf(r).Elem()
							if vv.FieldByName("Ok").Interface().(bool) {
								ui.MakeMsg(gui, sm.scene, "meta-add-ok", "add Ok", func() {
									sm.scene.Pop(gui)
									sm.scene.Pop(gui)
								}, nil)
							} else {
								ui.MakeMsg(gui, sm.scene, "meta-add-error", vv.FieldByName("Reason").Interface().(string), func() {
									sm.scene.Pop(gui)
								}, nil)
							}
						}
						return nil
					})
				}()
			}
		})
		return nil
	}

	viewTables.AddKey(gocui.KeyCtrlA, onCltA)
	viewFields.AddKey(gocui.KeyCtrlA, onCltA)

	/*
		onCltR := func(_ *gocui.Gui, _ *gocui.View) error {
			var hint string
			switch sd.scene.Bottom().GetActiveView().Name {
			case "depmnt-sets":
				if _, set := sd.getSelectedSet(); nil == set {
					return nil
				} else {
					hint = fmt.Sprintf("Do you really want to remove set:%d?", set.Id)
				}
			case "depmnt-nodes":
				if _, node := sd.getSelectedNode(); nil == node {
					return nil
				} else {
					hint = fmt.Sprintf("Do you really want to remove node:%d ?", node.Id)
				}
			}

			ui.MakeMsg(gui, sd.scene, "depmnt-remove-confirm", hint, func() {
				sd.scene.Pop(gui)
				req, resp, err := sd.makeRemReq()
				if nil != err {
					ui.MakeMsg(gui, sd.scene, "depmnt-rem-error", err.Error(), func() {
						sd.scene.Pop(gui)
					}, nil)
				} else {
					ui.MakeMsg(gui, sd.scene, "depmnt-rem-wait-response", "Waitting response from server...", nil, nil)
					go func() {
						r, err := sd.httpcli.Call(req, resp)
						gui.Update(func(_ *gocui.Gui) error {
							sd.scene.Pop(gui)
							if nil != err {
								ui.MakeMsg(gui, sd.scene, "depmnt-rem-error", err.Error(), func() {
									sd.scene.Pop(gui)
								}, nil)
							} else {
								vv := reflect.ValueOf(r).Elem()
								if vv.FieldByName("Ok").Interface().(bool) {
									ui.MakeMsg(gui, sd.scene, "depmnt-rem-ok", "remove Ok", func() {
										sd.scene.Pop(gui)
									}, nil)
								} else {
									ui.MakeMsg(gui, sd.scene, "depmnt-rem-error", vv.FieldByName("Reason").Interface().(string), func() {
										sd.scene.Pop(gui)
									}, nil)
								}
							}
							return nil
						})
					}()
				}
			}, func() {
				sd.scene.Pop(gui)
			})

			return nil
		}

		viewSets.AddKey(gocui.KeyCtrlR, onCltR)
		viewNodes.AddKey(gocui.KeyCtrlR, onCltR)*/

	sm.scene.Push(layer)
}

func newMeta(gui *gocui.Gui, httpcli *consoleHttp.Client) *sceneMeta {
	sm := &sceneMeta{
		httpcli: httpcli,
		scene:   &ui.Scene{Name: "meta"},
	}
	return sm
}
