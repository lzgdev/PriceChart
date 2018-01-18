
var coll_name, coll_set;
use bfx-pub;

db.getCollectionNames();

coll_name = 'set-colls';
coll_set  = db.getCollection(coll_name);
coll_set.find();

coll_name = null;
coll_name = "";

if ((coll_name != null) && (coll_name != "")) {
  coll_set = db.getCollection(coll_name);
  coll_set.find();
}
