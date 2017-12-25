
use bfx-pub

db.getCollectionNames()

coll_name = 'set-colls'
//coll_name = 'book-P0-20171225000000'
coll_name = 'candles-1m-20171224210000'

coll_set = db.getCollection(coll_name)

coll_set.find()

