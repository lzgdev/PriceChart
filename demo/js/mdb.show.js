
var coll_name, coll_set;
use bfx-pub;

db.getCollectionNames();

coll_name = 'set-colls';
coll_set  = db.getCollection(coll_name);
coll_set.find();

coll_name = "";
//coll_name = "trades-tBTCUSD";
coll_name = "candles-tBTCUSD-1m";

if (coll_name != "") {
  coll_set = db.getCollection(coll_name);
  //coll_set.find();
  coll_set.find().sort({_id:-1});
}

