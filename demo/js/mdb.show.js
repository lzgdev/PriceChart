
use bfx-pub

db.getCollectionNames()

coll_name = 'set-colls'
//coll_name = 'book-P0-20171225000000'
//coll_name = 'book-P3-20171229090000'

coll_set = db.getCollection(coll_name)

coll_set.find()

