
use bfx-pub

db.getCollectionNames()

coll_name = 'ticker-201712060430'
//coll_name = 'book-P1-201712060300'
//coll_name = 'set-colls'

coll_set = db.getCollection(coll_name)

coll_set.find()

