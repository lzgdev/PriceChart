// module kkai-dev52

/*
import { ClChanData_ABooks, ClChanData_Array, ClChanData, } from './kkai-dev11.js';
// */

var MongoClient = require('mongodb').MongoClient;

class ClChanData_DbBase
{
  constructor()
  {
/*
	def __init__(self, prec, size):
		self.fdbg_db = False
		super(TradeBook_DbBase, self).__init__(prec, size)
		self.db_collection = None
		self.dbid_price_bids = None
		self.dbid_price_asks = None
 */
    this.db_database    = null;
    this.db_collection  = null;
  }

  onDb_PrepConnect(db_url, name_collection)
  {
    if (this.db_database != null) {
      this.onDb_PrepCollection(name_collection);
    }
    else {
      MongoClient.connect(db_url, (err, db) => {
        if (!err) {
          this.db_database = db;
          this.onDb_PrepCollection(name_collection);
        }
        if (err) throw err;
      });
    }
  }

  onDb_PrepCollection(name_collection)
  {
console.log("ClChanData_DbBase(onDb_PrepCollection): 1");
    if (this.db_collection != null) {
      this.onDb_Prepared();
    }
    else {
      this.db_database.collection(name_collection, { strict: true, }, (err2, col2) => {
console.log("ClChanData_DbBase(onDb_PrepCollection): 2, err:", err2);
        if (!err2) {
          this.db_collection  = col2;
          this.onDb_Prepared();
console.log("ClChanData_DbBase(onDb_PrepCollection): 22");
        }
        else {
          this.db_database.createCollection(name_collection, (err3, col3) => {
console.log("ClChanData_DbBase(onDb_PrepCollection): 3, err:", err3);
            if (!err3) {
              this.db_collection = col3;
              this.onDb_Prepared();
            }
            if (err3) throw err3;
          });
        }
      });
    }
  }

  onDb_Prepared()
  {
console.log("ClChanData_DbBase(onDb_Prepared): ", this.db_collection);
    // Insert a document in the capped collection
    this.db_collection.insertOne({a:1}, {w:1}, (err, result) => {
console.log("ClChanData_DbBase(onDb_Prepared): insertOne");
        this.db_database.close();
        this.db_database = null;
      });
  }

//var url = "mongodb://localhost:27017/mydb";
  dbConnect(db_url, name_collection)
  {
    this.onDb_PrepConnect(db_url, name_collection);
  }

/*
	def dbInit(self, db_database):
		if self.fdbg_db:
			print("TradeBook_DbBase(dbInit): ...")
		flag_exist = True if (self.wreq_prec in db_database.collection_names(False)) else False
		self.db_collection = None if not flag_exist else pymongo.collection.Collection(
							db_database, self.wreq_prec, False)
		if self.db_collection == None:
			return
		rec_tmp = self.db_collection.find_one({ DBNAME_BOOK_RECTYPE: 'price.extr.bids', })
		if rec_tmp != None:
			self.dbid_price_bids = rec_tmp['_id']
		rec_tmp = self.db_collection.find_one({ DBNAME_BOOK_RECTYPE: 'price.extr.asks', })
		if rec_tmp != None:
			self.dbid_price_asks = rec_tmp['_id']

	def dbLoad_Books(self):
		if self.db_collection == None:
			return
		# load TradeBook of bids
		crr_recs = self.db_collection.find({ DBNAME_BOOK_RECTYPE: 'book.bids', })
		for doc_rec in crr_recs:
			rec_update = [ doc_rec['book-price'], doc_rec['book-count'], doc_rec['book-amount'], ]
			self.upBookRec(rec_update)
		crr_recs.close()
		# load TradeBook of asks
		crr_recs = self.db_collection.find({ DBNAME_BOOK_RECTYPE: 'book.asks', })
		for doc_rec in crr_recs:
			rec_update = [ doc_rec['book-price'], doc_rec['book-count'], doc_rec['book-amount'], ]
			self.upBookRec(rec_update)
		crr_recs.close()
  */

  dbLoad_Books()
  {
  }
}

class ClChanData_DbWriter extends ClChanData_DbBase
{
/*
//class TradeBook_DbWrite(TradeBook_DbBase):
	def __init__(self, prec, size):
		self.fdbg_dbwr = False
		super(TradeBook_DbWrite, self).__init__(prec, size)

	def dbInit(self, db_database):
		super(TradeBook_DbWrite, self).dbInit(db_database)
		if (self.db_collection == None):
			self.db_collection = pymongo.collection.Collection(db_database, self.wreq_prec, True)
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new collection(write).")
		if (self.db_collection != None) and (self.dbid_price_bids == None):
			self.dbid_price_bids = self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.bids', }).inserted_id
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new price:bids(write).")
		if (self.db_collection != None) and (self.dbid_price_asks == None):
			self.dbid_price_asks = self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.asks', }).inserted_id
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new price:asks(write).")

	def upBookRec_end_ex(self, rec_update, rec_del, rec_new, rec_up):
		price_rec = rec_update[0]
		flag_bids = True if rec_update[2] >  0.0 else False
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		list_update = self.loc_book_bids if flag_bids else self.loc_book_asks
		# update database
		price_min = None if len(list_update) == 0 else list_update[0][0]
		price_max = None if len(list_update) == 0 else list_update[len(list_update)-1][0]
		if flag_bids:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_bids, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		else:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_asks, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		str_rec_type = 'book.bids' if flag_bids else 'book.asks'
		if   rec_del:
			self.db_collection.delete_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, })
		elif rec_new:
			self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec,
						'book-count': rec_update[1], 'book-amount': rec_update[2], })
		elif rec_up:
			self.db_collection.find_one_and_update({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, },
						{ '$set': { 'book-count': rec_update[1], 'book-amount': rec_update[2], } })

	def upBookRecs_bgn_ex(self, recs_update):
		if (self.db_collection == None):
			return
		self.db_collection.delete_many({ DBNAME_BOOK_RECTYPE: 'book.bids', })
		self.db_collection.delete_many({ DBNAME_BOOK_RECTYPE: 'book.asks', })
  */
}

module.exports = {
  ClChanData_DbWriter: ClChanData_DbWriter,
  ClChanData_DbBase: ClChanData_DbBase,
}

