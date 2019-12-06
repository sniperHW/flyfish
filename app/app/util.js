var util = {};
util.httpGet = function(url,success,error){
    $.ajax({
        url:url,
        type: "get",
        async: true,
        success: success,
        error: error
    });
};

util.httpPost = function(url,data,success,error){
    $.ajax({
        url:url,
        type: "post",
        async: true,
        dataType: "json",
        data:data,
        success: success,
        error: error
    });
};

//弹出一个询问框，有确定和取消按钮
util.firm = function(msg,firmFunc) {
    //利用对话框返回的值 （true 或者 false）
    if (confirm(msg) ){
        firmFunc()
    }
};

// 字符串格式化
util.format = function(src){
    if (arguments.length == 0) return null;
    let args = Array.prototype.slice.call(arguments, 1);
    return src.replace(/\{(\d+)\}/g, function(m, i){
        return args[i];
    });
};

// 设置cookie的函数  （名字，值，过期时间（天））
util.setCookie = function (cname, cvalue, exdays) {
    let d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    let expires = "expires=" + d.toUTCString();
    document.cookie = cname + "=" + cvalue + "; " + expires;
};

//获取cookie
//取cookie的函数(名字) 取出来的都是字符串类型 子目录可以用根目录的cookie，根目录取不到子目录的 大小4k左右
util.getCookie = function(cname) {
    let name = cname + "=";
    let ca = document.cookie.split(';');
    for(let i=0; i<ca.length; i++)
    {
        let c = ca[i].trim();
        if (c.indexOf(name)===0) return c.substring(name.length,c.length);
    }
    return "";
};


util.strToConf = function (str) {
  let ret = new Map();
  let s = str.split(",");
  for (let i = 0;i < s.length;i++){
      let item = s[i].split(":");
      ret.set(item[0],{column:item[0],tt:item[1],def:item[2]})
  }
  return ret;
};

util.confToStr = function (conf) {
    let arr = new Array();
    for (let [key,item] of  conf) {
        arr.push(util.format("{0}:{1}:{2}", item.column, item.tt, item.def));
    }
    let ret = arr.join(",");
    return ret;
};

util.dataToconfStr = function (data) {
    let arr = new Array();
    for (let i =0 ;i < data.length;i++){
        let item = data[i];
        if (item.get("域名") !== "" && item.get("类型") !== ""){
            arr.push(util.format("{0}:{1}:{2}",item.get("域名"),item.get("类型"),item.get("默认值")))
        }
    }
    let ret = arr.join(",");
    return ret;
};

util.dataToDataMap = function (tableName,data) {
    let conf = database.confs.get(tableName);
    let ret = new Array();
    for (let i = 0;i < data.length;i++){
        let row = data[i];
        if (row.get("__key__") !== "") {
            let rows = new Array();
            for (let [key, v] of row) {
                if (key === "__key__") {
                    rows.push({column:key,tt:"string",value:v})
                }else {
                    if (v !== "") {
                        let tt = conf.get(key).tt;
                        rows.push({column: key, tt: tt, value: v})
                    }
                }
            }
            if (rows.length > 1) {
                ret.push(rows)
            }
        }
    }
    return ret
};