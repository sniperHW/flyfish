var mTable = {};
mTable.context = null;
mTable.table = {};

mTable.new = function (context,header) {
    mTable.context = context;

    context.innerHTML = "";
    let mHtml = "";
    let mth = `<thead><tr>`;
    for (let i =0;i<header.length;i++){
        mth += util.format(`<th>{0}</th>`,header[i])
    }
    mth += `</tr></thead><tbody></tbody>`;
    mHtml += mth;
    context.innerHTML = mHtml;
    mTable.insertRow();
};

mTable.insertRow = function(){
    let mbo =  $(mTable.context).children("tbody");
    let len = $(mTable.context).children("thead").children("tr").children("th").length;
    let trs = `<tr>`;
    for (let i=0;i<len;i++){
        trs += `<td contenteditable="true"></td>`
    }
    trs += `</tr>`;
    mbo.append(trs)
};

mTable.getHeaderNumber = function(hStr){
    let mth = $(mTable.context).children("thead").children("tr").children("th");
    for (let i=0;i<mth.length;i++){
        let h = mth[i];
        if (h.innerHTML === hStr){
            return i
        }
    }
    return null;
};

mTable.getColumnToHeader = function(col){
    let mth = $(mTable.context).children("thead").children("tr").children("th");
    return mth[col].innerHTML;
};

mTable.getCell = function(row , col){
    let mtr = $(mTable.context).children("tbody").children("tr");
    let mtd = $(mtr[row]).children("td");
    return mtd[col]
};

mTable.getCellValue = function(row , col){
    let cell = mTable.getCell(row,col);
    return cell.innerHTML
};

mTable.setCellValue = function(row,col,value){
    let cell = mTable.getCell(row,col);
    cell.innerHTML = value;
};

mTable.loadData = function (data) {
    let mtr = $(mTable.context).children("tbody").children("tr");

    for (let i = mtr.length;i < data.length;i++){
        mTable.insertRow()
    }
    //console.log(data);
    for (let i =0;i < data.length;i ++ ){
        for (let j = 0; j < data[i].length;j++){
            let item = data[i][j];
            mTable.setCellValue(i,mTable.getHeaderNumber(item.column),item.value);
        }
    }
};

mTable.getData = function () {
    let ret = new Array();
    let mtr = $(mTable.context).children("tbody").children("tr");
    for (let i = 0;i < mtr.length;i++){
        let row = new Map();
        let mtd = $(mtr[i]).children("td");
        for (let j =0;j < mtd.length;j++){
            let k_ =  mTable.getColumnToHeader(j);
            row.set(k_,mTable.getCellValue(i,j));
        }
        ret.push(row);
    }
    return ret;
};

mTable.getDataLine2 = function () {
    let ret = new Array();
    let mtr = $(mTable.context).children("tbody").children("tr");
    for (let i = 1;i < mtr.length;i++){
        let row = new Map();
        let mtd = $(mtr[i]).children("td");
        for (let j =0;j < mtd.length;j++){
            let k_ =  mTable.getColumnToHeader(j);
            row.set(k_,mTable.getCellValue(i,j));
        }
        ret.push(row);
    }
    return ret;
};