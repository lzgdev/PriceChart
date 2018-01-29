
var coll_name, coll_set;
use bfx-pub;

coll_name = "";
//coll_name = "set-colls";

if (coll_name != "") {
  db.getCollectionNames();
  coll_set  = db.getCollection(coll_name);
  coll_set.find();
}

coll_name = "";
//coll_name = "ticker-tBTCUSD";
coll_name = "trades-tBTCUSD";
//coll_name = "book-tBTCUSD-P0";
//coll_name = "candles-tBTCUSD-1m";

if (coll_name != "") {
  coll_set = db.getCollection(coll_name);
  //coll_set.find();
  coll_set.find().sort({_id:-1});
}

