// module kkai-dev52

/*
import { ClDataSet_ABooks, ClDataSet_Array, } from './kkai-dev11.js';
// */

var MongoClient = require('mongodb').MongoClient;

var _CollName_CollSet = 'set-colls';

class ClDataSet_DbBase
{
  constructor()
  {
    this.db_database    = null;
    this.db_coll_set    = null;
    this.db_collections = { };
    this.db_docs_toadd  = [ ];
  }

  dbChk_IsDbReady() {
    return (this.db_database != null) ? true : false;
  }

  dbChk_IsCollReady(name_coll) {
    return (((name_coll == _CollName_CollSet) && (this.db_coll_set != null)) ||
            ((name_coll != null) &&
             this.db_collections.hasOwnProperty(name_coll))) ? true : false;
  }

  dbChk_IsReady() {
    return (this.db_coll_set != null) ? true : false;
  }

//var url = "mongodb://localhost:27017/mydb";
  dbOP_Connect(db_url, name_coll)
  {
    this.onDbOP_Connect_impl(db_url, name_coll);
  }

  dbOP_Close()
  {
    this.onDbOP_Close_impl();
  }

  dbOP_LoadBooks()
  {
  }

  dbOP_AddColl(name_coll)
  {
    this.onDbOP_AddColl_impl(name_coll);
  }

  dbOP_AddDoc(name_coll, obj_doc)
  {
    if ((name_coll == null) || (obj_doc == null)) {
      return false;
    }
    else
    if (!this.dbChk_IsCollReady(name_coll)) {
      this.db_docs_toadd.push({ coll: name_coll, doc: obj_doc, });
    }
    else {
      this.onDbOP_AddDoc_impl(name_coll, obj_doc);
    }
    return true;
  }

  onDbEV_AddDoc(name_coll, obj_doc, result)
  {
    console.log("ClDataSet_DbBase(onDbEV_AddDoc): coll:", name_coll, "result:", result.insertedId, "doc:", JSON.stringify(obj_doc));
  }

  onDbEV_RunNext(prep_arg1)
  {
    // Insert a document in the capped collection
    for (var i=0; i <  this.db_docs_toadd.length; i++)
    {
      var name_coll, obj_doc;
      name_coll = this.db_docs_toadd[i].coll;
      if (!this.dbChk_IsCollReady(name_coll)) {
        continue;
      }
      obj_doc = this.db_docs_toadd[i].doc;
      this.db_docs_toadd.splice(i, 1);
      this.onDbOP_AddDoc_impl(name_coll, obj_doc);
      break;
    }
  }

  onDbEV_Closed()
  {
    console.log("ClDataSet_DbBase(onDbEV_Closed): db closed.");
  }

  onDbOP_Connect_impl(db_url, name_coll)
  {
    if (this.db_database != null) {
      this.dbOP_AddColl(_CollName_CollSet);
    }
    else {
      MongoClient.connect(db_url, (err, db) => {
        if (err == null) {
          console.log("ClDataSet_DbBase(onDbOP_Connect_impl): db connected to", db_url);
          this.db_database = db;
          this.dbOP_AddColl(_CollName_CollSet);
        }
        if (err) throw err;
      });
    }
  }

  onDbOP_Close_impl()
  {
    console.log("ClDataSet_DbBase(onDbOP_Close_impl): ...");
    if (this.db_database == null) {
      return false;
    }
    this.db_database.close((err, result) => {
        if (err != null) {
          this.db_collections = { };
          this.db_database  = null;
          this.db_coll_set  = null;
          this.onDbEV_Closed();
        }
      });
    return true;
  }

  onDbOP_AddColl_impl(name_coll)
  {
    if (name_coll == null) {
      return false;
    }
    else
    if (this.dbChk_IsCollReady(name_coll)) {
      this.onDbEV_RunNext(11);
    }
    this.db_database.collection(name_coll, { strict: true, }, (err2, col2) => {
        if (err2 == null) {
          if (name_coll != _CollName_CollSet) {
            this.db_collections[name_coll] = col2;
          }
          else {
            this.db_coll_set = col2;
          }
          this.onDbEV_RunNext(21);
        }
        else {
          this.db_database.createCollection(name_coll, (err3, col3) => {
              if (err3 == null) {
                if (name_coll != _CollName_CollSet) {
                  this.db_collections[name_coll] = col3;
                }
                else {
                  this.db_coll_set = col3;
                }
                this.onDbEV_RunNext(31);
              }
            });
        }
      });
  }

  onDbOP_AddDoc_impl(name_coll, obj_doc)
  {
    if ((obj_doc == null) || !this.dbChk_IsCollReady(name_coll)) {
      return;
    }
    this.db_collections[name_coll].insertOne(obj_doc, null, (err, result) => {
        if ((err == null) && (name_coll != _CollName_CollSet)) {
          this.onDbEV_AddDoc(name_coll, obj_doc, result)
        }
        else
        if (err != null) {
          console.log("ClDataSet_DbBase(onDbOP_AddDoc_impl) err:", err);
        }
        this.onDbEV_RunNext(51);
      });
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
		if (self.db_coll_set == None):
			self.db_coll_set = pymongo.collection.Collection(db_database, self.wreq_prec, True)
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new collection(write).")
		if (self.db_coll_set != None) and (self.dbid_price_bids == None):
			self.dbid_price_bids = self.db_coll_set.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.bids', }).inserted_id
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new price:bids(write).")
		if (self.db_coll_set != None) and (self.dbid_price_asks == None):
			self.dbid_price_asks = self.db_coll_set.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.asks', }).inserted_id
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
			self.db_coll_set.find_one_and_update( { '_id': self.dbid_price_bids, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		else:
			self.db_coll_set.find_one_and_update( { '_id': self.dbid_price_asks, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		str_rec_type = 'book.bids' if flag_bids else 'book.asks'
		if   rec_del:
			self.db_coll_set.delete_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, })
		elif rec_new:
			self.db_coll_set.insert_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec,
						'book-count': rec_update[1], 'book-amount': rec_update[2], })
		elif rec_up:
			self.db_coll_set.find_one_and_update({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, },
						{ '$set': { 'book-count': rec_update[1], 'book-amount': rec_update[2], } })

	def upBookRecs_bgn_ex(self, recs_update):
		if (self.db_coll_set == None):
			return
		self.db_coll_set.delete_many({ DBNAME_BOOK_RECTYPE: 'book.bids', })
		self.db_coll_set.delete_many({ DBNAME_BOOK_RECTYPE: 'book.asks', })
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

