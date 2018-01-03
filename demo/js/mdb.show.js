
use bfx-pub

db.getCollectionNames()

coll_name = 'set-colls'
coll_set = db.getCollection(coll_name)
coll_set.find()

coll_name = null
//coll_name = 'book-P0-20180102030000'

if (coll_name != null) {
  coll_set = db.getCollection(coll_name);
  coll_set.find();
}

