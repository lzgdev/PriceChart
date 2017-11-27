// module kkai-dev52

/*
import { ClDataSet_ABooks, ClDataSet_Array, } from './kkai-dev11.js';
// */

var MongoClient = require('mongodb').MongoClient;

class ClDataSet_DbBase
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
        console.log("ClDataSet_DbBase(onDb_PrepConnect): err:", err);
        if (err == null) {
          this.db_database = db;
          this.onDb_PrepCollection(name_collection);
        }
        if (err) throw err;
      });
    }
  }

  onDb_PrepCollection(name_collection)
  {
    if (this.db_collection != null) {
      this.onDb_RunPrepared(11);
    }
    else {
      this.db_database.collection(name_collection, { strict: true, }, (err2, col2) => {
        if (err2 == null) {
          this.db_collection = col2;
          this.onDb_RunPrepared(21);
        }
        else {
          this.db_database.createCollection(name_collection, (err3, col3) => {
            if (err3 == null) {
              this.db_collection = col3;
              this.onDb_RunPrepared(31);
            }
            if (err3) throw err3;
          });
        }
      });
    }
  }

  onDb_RunPrepared(prep_arg1)
  {
    console.log("ClDataSet_DbBase(onDb_RunPrepared): arg1:", prep_arg1);
    // Insert a document in the capped collection
/*
    this.db_collection.insertOne({a:1}, {w:1}, (err, result) => {
        console.log("ClDataSet_DbBase(onDb_RunPrepared): insertOne");
        this.db_database.close(true, (err1, result1) => {
            this.onDb_Close(err1, result1);
          });
      });
// */
  }

  onDb_Close(err, result)
  {
    console.log("ClDataSet_DbBase(onDb_Close): err:", err, "result:", result);
    this.db_database = null;
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

class ClDataSet_DbWriter extends ClDataSet_DbBase
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

/*
class AdpBitfinexWSS(websocket.WebSocketApp):
	def __init__(self, url_bfx, sio_namespace, db_client, api_key=None, api_secret=None):
		super(AdpBitfinexWSS, self).__init__(url_bfx)
		self.on_open = AdpBitfinexWSS.wssEV_OnOpen
		self.on_message = AdpBitfinexWSS.wssEV_OnMessage
		self.on_error = AdpBitfinexWSS.wssEV_OnError
		self.on_close = AdpBitfinexWSS.wssEV_OnClose
		self.auth_api_key = api_key
		self.auth_api_secret = api_secret
		db_database = db_client['books']
		self.book_p0_book = TradeBook_DbWrite("P0", 25)
		self.book_p0_book.dbInit(db_database)
		self.book_p1_book = TradeBook_DbWrite("P1", 25)
		self.book_p1_book.dbInit(db_database)

	def	wssEV_OnOpen(self):
		if (self.auth_api_key != None) and (self.auth_api_secret != None):
			self.wss_auth(self.auth_api_key, self.auth_api_secret)
#		self.send('{ "event": "subscribe", "channel": "ticker", "symbol": "tBTCUSD" }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "R0", "len": 100 }')
		obj_wreq = {
				'event': 'subscribe', 'channel': 'book', 'symbol': 'tBTCUSD',
				'prec': self.book_p0_book.wreq_prec,
				'freq': 'F0',
				'len':  self.book_p0_book.wreq_len,
			}
		self.send(json.dumps(obj_wreq))
		run_times = 15
		def run(*args):
			for i in range(run_times):
				time.sleep(1)
				print("wait ...")
			time.sleep(1)
			self.close()
			print("thread terminating...")
		_thread.start_new_thread(run, ())

	def	wssEV_OnMessage(self, message):
		rsp_obj  = json.loads(message)
		if type(rsp_obj) is dict and 'event' in rsp_obj:
			rsp_dict = rsp_obj
			rsp_list = None
		elif type(rsp_obj) is list:
			rsp_dict = None
			rsp_list = rsp_obj
		else:
			rsp_dict = None
			rsp_list = None
		if   not rsp_list is None and len(rsp_list) == 2:
			chan_id = rsp_list[0]
			if   not type(rsp_list[1]) is list:
				print("msg(misc): ", message)
			elif self.book_p0_book.wrsp_chan == chan_id:
				self.book_p0_book.upBookRecs(rsp_list[1])
			elif self.book_p1_book.wrsp_chan == chan_id:
				self.book_p1_book.upBookRecs(rsp_list[1])
			else:
				print("msg(list): ", message)
		elif not rsp_dict is None and rsp_dict['event'] == 'subscribed':
			if   rsp_dict['channel'] == 'book' and rsp_dict['prec'] == 'P0':
				self.book_p0_book.upBookChan(rsp_dict['chanId'])
			elif rsp_dict['channel'] == 'book' and rsp_dict['prec'] == 'P1':
				self.book_p1_book.upBookChan(rsp_dict['chanId'])
			print("msg(dict): ", rsp_dict['event'], message)

	def	wssEV_OnError(self, error):
		print(error)

	def	wssEV_OnClose(self):
		self.book_p0_book.dbgOut_Books()
		print("### closed ###")

	def wss_auth(self, api_key, api_secret):
		auth_nonce = int(time.time() * 1000000)
		auth_payload = 'AUTH{}'.format(auth_nonce)
		auth_signature = hmac.new(
				api_secret.encode(),
				msg = auth_payload.encode(),
				digestmod = hashlib.sha384
			).hexdigest()
		json_payload = {
				'event': 'auth',
				'apiKey': api_key,
				'authSig': auth_signature,
				'authPayload': auth_payload,
				'authNonce': auth_nonce,
				'calc': 1
			}
		self.send(json.dumps(json_payload))
// */

module.exports = {
  ClDataSet_DbWriter: ClDataSet_DbWriter,
  ClDataSet_DbBase: ClDataSet_DbBase,
}

