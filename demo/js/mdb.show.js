
var coll_name, coll_set;
//use bfx-pub;
//use bfx-down1;
use bfx-down;

coll_name = "";
//coll_name = "set-colls";

if (coll_name != "") {
  db.getCollectionNames();
  coll_set  = db.getCollection(coll_name);
  coll_set.find();
}

coll_name = "";
//coll_name = "ticker-tBTCUSD";
//coll_name = "trades-tBTCUSD";
//coll_name = "book-tBTCUSD-P0";
coll_name = "candles-tBTCUSD-1m";

if (coll_name != "") {
  coll_set = db.getCollection(coll_name);
  //coll_set.find();
  coll_set.find().sort({$natural: 1}).limit(20);
  //coll_set.find({mts: { $lte: 1511052480000 } }).limit(20).sort({$natural: 1});
}

