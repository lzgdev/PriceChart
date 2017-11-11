import websocket
import _thread
import time

import hmac
import hashlib
import json

import pymongo

from pymongo import MongoClient

from adapters.basebooks import TradeBook_DbBase
from adapters.basebooks import DBNAME_BOOK_PRICEEXTR, DBNAME_BOOK_RECTYPE

class TradeBook_DbWrite(TradeBook_DbBase):
	def __init__(self, db_database, prec, size):
		self.fdbg_dbwr = False
		super(TradeBook_DbWrite, self).__init__(db_database, prec, size)
		# database members init
		self.dbInit(db_database)

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

	def dbClear(self):
		if (self.db_collection == None):
			return
		self.db_collection.drop()
		if self.fdbg_dbwr:
			print("TradeBook_DbWrite(dbClear): drop collection(write).")

	def upBookRec_ex(self, rec_update, rec_del, rec_new, rec_up):
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

class AdpBitfinexWSS(websocket.WebSocketApp):
	def __init__(self, url, db_client, api_key=None, api_secret=None):
		super(AdpBitfinexWSS, self).__init__(url)
		self.on_open = AdpBitfinexWSS.wssEV_OnOpen
		self.on_message = AdpBitfinexWSS.wssEV_OnMessage
		self.on_error = AdpBitfinexWSS.wssEV_OnError
		self.on_close = AdpBitfinexWSS.wssEV_OnClose
		self.auth_api_key = api_key
		self.auth_api_secret = api_secret
		db_database = db_client['books']
		self.book_p0_book = TradeBook_DbWrite(db_database, "P0", 25)
		self.book_p1_book = TradeBook_DbWrite(db_database, "P1", 25)

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

