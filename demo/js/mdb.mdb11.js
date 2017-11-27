
//import { ClChanData_DbWriter, ClChanData_DbBase, } from '../../code/js/kkai-dev52.js'

var dev52 = require('../../code/js/kkai-dev52.js');

var db_url = "mongodb://localhost:27017/mydb";
var col_name = "customers";

var dbObj = null;

console.log("main(1) ...");

dbObj = new dev52.ClChanData_DbWriter();

dbObj.dbConnect(db_url, col_name);

console.log("main(99), dbObj:", dbObj, "db:", dbObj.db_database, "col:", dbObj.db_collection);

