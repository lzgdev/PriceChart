// module kkai-dev52

/*
import { ClDataSet_ABooks, ClDataSet_Array, } from './kkai-dev11.js';
// */

var MongoClient = require('mongodb').MongoClient;

var _CollName_CollSet = 'set-colls';

class ClDataSet_DbBase
{
  constructor()
  {
    this.db_dburl = null;
    this.db_database    = null;
    this.db_coll_set    = null;
    this.db_collections = { };
    this.db_todo_count  = 0;
    this.db_todo_coll_add    = [ ];
    this.db_todo_coll_load   = [ ];
    this.db_todo_docs_write  = [ ];
  }

  dbChk_IsDbReady() {
    return (this.db_database != null) ? true : false;
  }

  dbChk_IsCollReady(name_coll) {
    return (((name_coll == _CollName_CollSet) && (this.db_coll_set != null)) ||
            ((name_coll != null) &&
             this.db_collections.hasOwnProperty(name_coll))) ? true : false;
  }

  dbOP_Connect(db_url)
  {
    if (this.db_database == null) {
      this.db_dburl = db_url;
      this.onDbOP_Connect_impl(this.db_dburl);
    }
  }

  dbOP_Close()
  {
    this.onDbOP_Close_impl();
  }

  dbOP_AddColl(name_coll, wreq_chan, wreq_args)
  {
    if (name_coll == null) {
      return false;
    }
    if (!this.dbChk_IsCollReady(name_coll)) {
      this.db_todo_coll_add.push({ coll: name_coll, channel: wreq_chan, reqargs: wreq_args, });
    }
    this._dbOP_RunNext(211);
    return true;
  }

  dbOP_AddDoc(name_coll, obj_doc)
  {
    if ((name_coll == null) || (obj_doc == null)) {
      return false;
    }
    this.db_todo_docs_write.push({ coll: name_coll, doc: obj_doc, });
    this._dbOP_RunNext(221);
    return true;
  }

  dbOP_LoadColl(name_coll, dataset, find_args, sort_args)
  {
    if (name_coll == null) {
      return false;
    }
    if (!this.dbChk_IsCollReady(name_coll)) {
      this.dbOP_AddColl(name_coll, null, null);
    }
    this.db_todo_coll_load.push({ coll: name_coll, dataset: dataset, find_args: find_args, sort_args: sort_args, });
    this._dbOP_RunNext(231);
    return true;
  }

  _dbOP_RunNext(run_arg0)
  {
    if (this.db_todo_count >= 2) {
      return false;
    }
    this.db_todo_count ++;
    setImmediate(() => {
        this.onDbEV_RunNext(run_arg0);
        this.db_todo_count --;
      });
    return true;
  }

  onDbEV_AddColl(name_coll, result)
  {
    console.log("ClDataSet_DbBase(onDbEV_AddColl): coll:", name_coll);
  }
  onDbEV_AddDoc(name_coll, obj_doc, result)
  {
    console.log("ClDataSet_DbBase(onDbEV_AddDoc): coll:", name_coll, "result:", result.insertedId, "doc:", JSON.stringify(obj_doc));
  }

  onDbEV_RunNext(run_arg0)
  {
    var  i;
    var  flag_next = false;
    if (this.db_database == null) {
      return false;
    }
//    console.log("ClDataSet_DbBase(onDbEV_RunNext), arg0:", run_arg0, "count:", this.db_todo_count);
    // try to add collections
    while (this.db_todo_coll_add.length > 0)
    {
      var  rec_add, name_coll;
      rec_add   = this.db_todo_coll_add.pop();
      name_coll = rec_add['coll'];
      if (this.dbChk_IsCollReady(name_coll)) {
        continue;
      }
      this.onDbOP_AddColl_impl(name_coll, rec_add['channel'], rec_add['reqargs']);
    }
    // try to load documents from a collection
    for (i=0; i <  this.db_todo_coll_load.length;  i++)
    {
      var name_coll, dataset, find_args, sort_args;
      name_coll = this.db_todo_coll_load[i].coll;
      if (!this.dbChk_IsCollReady(name_coll)) {
        continue;
      }
      dataset   = this.db_todo_coll_load[i].dataset;
      find_args = this.db_todo_coll_load[i].find_args;
      sort_args = this.db_todo_coll_load[i].sort_args;
      this.db_todo_coll_load.splice(i, 1);
      if (dataset != null) {
        this.onDbOP_LoadColl_impl(name_coll, dataset, find_args, sort_args);
      }
      break;
    }
    // try to add a document to a collection
    for (i=0; i <  this.db_todo_docs_write.length; i++)
    {
      var name_coll, obj_doc;
      name_coll = this.db_todo_docs_write[i].coll;
      if (!this.dbChk_IsCollReady(name_coll)) {
        continue;
      }
      obj_doc = this.db_todo_docs_write[i].doc;
      this.db_todo_docs_write.splice(i, 1);
      this.onDbOP_AddDoc_impl(name_coll, obj_doc);
      break;
    }
    return true;
  }

  onDbEV_Closed()
  {
    //console.log("ClDataSet_DbBase(onDbEV_Closed): db closed.");
  }

  onDbOP_Connect_impl(db_url)
  {
    MongoClient.connect(db_url, { }, (err, db) => {
        if (err == null) {
          console.log("ClDataSet_DbBase(onDbOP_Connect_impl): db connected to", db_url);
          this.db_database = db;
          this.dbOP_AddColl(_CollName_CollSet, null, null);
        }
        if (err) throw err;
        this._dbOP_RunNext(501);
      });
  }

  onDbOP_Close_impl()
  {
    console.log("ClDataSet_DbBase(onDbOP_Close_impl): ...");
    if (this.db_database == null) {
      return false;
    }
    this.db_database.close((err, result) => {
        if (err != null) {
          this.db_collections = { };
          this.db_database  = null;
          this.db_coll_set  = null;
          this.onDbEV_Closed();
        }
      });
    return true;
  }

  onDbOP_AddColl_impl(name_coll, wreq_chan, wreq_args)
  {
    this.db_database.collection(name_coll, { strict: true, }, (err2, col2) => {
        if (err2 == null)
        {
          if (name_coll == _CollName_CollSet) {
            this.db_coll_set = col2;
          }
          else {
            this.db_collections[name_coll] = col2;
          }
          this._dbOP_RunNext(511);
        }
        else
        if ((wreq_chan != null) && (wreq_args != null))
        {
          this.db_database.createCollection(name_coll, (err3, col3) => {
              if (err3 == null) {
                if (name_coll == _CollName_CollSet) {
                  this.db_coll_set = col3;
                }
                else {
                  this.db_collections[name_coll] = col3;
                  this.db_todo_docs_write.unshift({ coll: _CollName_CollSet, doc: {
                        coll: name_coll,
                        channel: wreq_chan,
                        reqargs: wreq_args,
                      }, });
                  this.onDbEV_AddColl(name_coll, this.db_collections[name_coll]);
                }
                this._dbOP_RunNext(512);
              }
            });
        }
    });
  }

  onDbOP_LoadColl_impl(name_coll, dataset, find_args, sort_args)
  {
    var db_cursor, db_coll = this.db_collections[name_coll];
    db_cursor = (sort_args == null) ? db_coll.find(find_args) : db_coll.find(find_args).sort(sort_args);
    db_cursor.forEach((obj_msg) => {
        dataset.locAppendData(1001, obj_msg);
      });
    this._dbOP_RunNext(521);
  }

  onDbOP_AddDoc_impl(name_coll, obj_doc)
  {
    var db_coll = (name_coll == _CollName_CollSet) ? this.db_coll_set :
                        this.db_collections[name_coll];
    db_coll.insertOne(obj_doc, null, (err, result) => {
        if ((err == null) && (name_coll != _CollName_CollSet)) {
          this.onDbEV_AddDoc(name_coll, obj_doc, result)
        }
        else
        if (err != null) {
          console.log("ClDataSet_DbBase(onDbOP_AddDoc_impl) err:", err);
        }
        this._dbOP_RunNext(531);
      });
  }
}

class ClDataSet_DbReader extends ClDataSet_DbBase
{
  constructor()
  {
    super();
  }

}

class ClDataSet_DbWriter extends ClDataSet_DbBase
{
  constructor()
  {
    super();
  }
}


module.exports = {
  ClDataSet_DbReader:   ClDataSet_DbReader,
  ClDataSet_DbWriter:   ClDataSet_DbWriter,
  ClDataSet_DbBase:     ClDataSet_DbBase,
}

