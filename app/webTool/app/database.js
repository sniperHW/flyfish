var database = {};
database.confs = new Map(); // tableName:
database.confStr = new Map();

database.clearConfs = function () {
    database.confs = new Map();
};

database.setConf = function (tableName,v) {
    database.confs.set(tableName,v)
};