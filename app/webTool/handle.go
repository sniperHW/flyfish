package webTool

import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/app/webTool/db"
	"github.com/sniperHW/flyfish/errcode"
	"io/ioutil"
	"net/http"
	"net/url"
)

/*
 * 测试链接
 * dbConfig:string,flyfishConfig:string
 */
func HandleTestConnection(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleTestConnection request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig":      "",
		"flyfishConfig": "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	_, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	_, err = GetFlyClient(args["flyfishConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 创建表
 * dbConfig:string,flyfishConfig:string,tableName:string,fields:string
 */
func HandleCreateTable(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleCreateTable request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig":      "",
		"flyfishConfig": "",
		"tableName":     "",
		"fields":        "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	client, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	fields, err := db.ProcessFields(client.GetType(), args["fields"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	// 1.创建表
	err = client.Create(args["tableName"], fields)
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	// 2.新增配置数据
	err = client.Set("table_conf", map[string]interface{}{
		"__table__": args["tableName"],
		"__conf__":  args["fields"],
	})
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	// 3.reload
	flyClient, err := GetFlyClient(args["flyfishConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	result := flyClient.ReloadTableConf().Exec()
	if result.ErrCode != errcode.ERR_OK {
		httpErr("同步到flyfish失败", w)
		return
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 新增列
 * dbConfig:string,flyfishConfig:string,tableName:string,fields:string
 */
func HandleAddColumn(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleAddColumn request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig":      "",
		"flyfishConfig": "",
		"tableName":     "",
		"fields":        "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	client, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	fields, err := db.ProcessFields(client.GetType(), args["fields"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	// 1.新增列
	err = client.Alter(args["tableName"], "ADD", fields)
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	// 2.追加数据
	ret, err := client.Get("table_conf", fmt.Sprintf("__table__ = '%s'", args["tableName"]), []string{"__conf__"})
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	err = client.Update("table_conf", fmt.Sprintf("__table__ = '%s'", args["tableName"]), map[string]interface{}{
		"__conf__": fmt.Sprintf("%s,%s", ret["__conf__"].(string), args["fields"]),
	})
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	// 3.reload
	flyClient, err := GetFlyClient(args["flyfishConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	result := flyClient.ReloadTableConf().Exec()
	if result.ErrCode != errcode.ERR_OK {
		httpErr("同步到flyfish失败", w)
		return
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 导出数据库表结构
 * dbConfig:string,tableName:string,isGetData:bool
 */
func HandleDumpSql(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleDumpSql request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig":  "",
		"tableName": "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	client, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	err = client.Truncate(args["tableName"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 清空表数据
 * dbConfig:string,tableName:string
 */
func HandleTruncate(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleTruncate request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig":  "",
		"tableName": "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	client, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	err = client.Truncate(args["tableName"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * table_conf表配置信息
 * dbConfig:string
 */
func HandleTableInfo(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleTableInfo request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"dbConfig": "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	client, err := db.GetClient(args["dbConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	resp := []map[string]interface{}{}
	// 查询表
	ret, err := client.GetAll("table_conf", []string{"__table__", "__conf__"})
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	for _, item := range ret {
		_item := map[string]interface{}{}
		for k, v := range item {
			_item[k] = v
		}

		// 表数据量
		count, _ := client.Count(item["__table__"].(string))
		_item["count"] = count
		resp = append(resp, _item)
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":     1,
		"tables": resp,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 添加数据
 * flyfishConfig:string,tableName:string,fields:[][]map[string]interface
 */
func HandleSet(w http.ResponseWriter, r *http.Request) {
	//_ = r.ParseForm()
	httpHeader(&w) //跨域

	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	r.Body.Close()

	var args map[string]interface{}
	err = json.Unmarshal(bytes, &args)
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	logger.Infoln("HandleSet request", r.Method, r.Form, args)

	tableName := args["tableName"].(string)
	flyClient, err := GetFlyClient(args["flyfishConfig"].(string))
	if err != nil {
		httpErr(err.Error(), w)
		return
	}
	tableDatas := args["tableData"].([]interface{})
	for i := 0; i < len(tableDatas); i++ {
		k, fields, err := processData(tableDatas[i])
		if err != nil {
			httpErr(err.Error(), w)
			return
		}
		result := flyClient.Set(tableName, k, fields).Exec()
		if result.ErrCode != errcode.ERR_OK {
			httpErr(fmt.Sprintf("flyfish ErrCode:%s", errcode.GetErrorStr(result.ErrCode)), w)
			return
		}
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 查询数据
 * flyfishConfig:string,tableName:string,key:string
 */
func HandleGet(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleGet request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"flyfishConfig": "",
		"tableName":     "",
		"key":           "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	flyClient, err := GetFlyClient(args["flyfishConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	result := flyClient.GetAll(args["tableName"], args["key"]).Exec()
	if result.ErrCode != errcode.ERR_OK {
		httpErr(fmt.Sprintf("flyfish ErrCode:%s", errcode.GetErrorStr(result.ErrCode)), w)
		return
	}

	ret := []map[string]interface{}{}
	for k, v := range result.Fields {
		ret = append(ret, map[string]interface{}{
			"column": k,
			"value":  v.GetValue(),
		})
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":     1,
		"result": ret,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

/*
 * 删除数据
 * flyfishConfig:string,tableName:string,key:string
 */
func HandleDel(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	logger.Infoln("HandleGet request", r.Method, r.Form)
	httpHeader(&w) //跨域

	args := map[string]string{
		"flyfishConfig": "",
		"tableName":     "",
		"key":           "",
	}
	if err := checkForm(r.Form, args); err != nil {
		httpErr(err.Error(), w)
		return
	}

	flyClient, err := GetFlyClient(args["flyfishConfig"])
	if err != nil {
		httpErr(err.Error(), w)
		return
	}

	result := flyClient.Del(args["tableName"], args["key"]).Exec()
	if result.ErrCode != errcode.ERR_OK {
		httpErr(fmt.Sprintf("flyfish ErrCode:%s", errcode.GetErrorStr(result.ErrCode)), w)
		return
	}

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": 1,
	}); err != nil {
		logger.Errorln("http resp err:", err)
	}
}

func checkForm(form url.Values, args map[string]string) error {
	for k := range args {
		v := form.Get(k)
		if v != "" {
			args[k] = v
		} else {
			return fmt.Errorf("key:%s not found\n", k)
		}
	}
	return nil
}

func httpHeader(w *http.ResponseWriter) {
	//跨域
	(*w).Header().Set("Access-Control-Allow-Origin", "*")             //允许访问所有域
	(*w).Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
	(*w).Header().Set("content-type", "application/json")             //返回数据格式是json
}

func httpErr(err string, w http.ResponseWriter) {
	logger.Infoln("httpErr", err)
	resp := map[string]interface{}{
		"ok":  0,
		"msg": err,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Errorln("http resp err:", err)
	}
}
