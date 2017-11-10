import websocket
import _thread
import time

import hmac
import hashlib
import json

import pymongo

from pymongo import MongoClient

class TradeBook(object):
	def __init__(self, db_database, prec, size):
		self.flag_dbg  = True
		self.flag_dbg  = False
		self.wreq_prec = prec
		self.wreq_len  = size
		self.wrsp_chan = None
		self.loc_book_bids = []
		self.loc_book_asks = []
		# database members init
		self.dbInit(db_database)

	def dbInit(self, db_database):
		flag_exist = True if (self.wreq_prec in db_database.collection_names(False)) else False
		self.db_collection = pymongo.collection.Collection(db_database, self.wreq_prec,
								False if flag_exist else True)
		rec_tmp = self.db_collection.find_one({ 'rec-type': 'price.extr.bids', })
		if rec_tmp != None:
			self.dbid_price_bids = rec_tmp['_id']
			print("TradeBook(dbInit)", self.wreq_prec, "extr-bids:", rec_tmp)
		else:
			self.dbid_price_bids = self.db_collection.insert_one({ 'rec-type': 'price.extr.bids', }).inserted_id
		rec_tmp = self.db_collection.find_one({ 'rec-type': 'price.extr.asks', })
		if rec_tmp != None:
			self.dbid_price_asks = rec_tmp['_id']
			print("TradeBook(dbInit)", self.wreq_prec, "extr-asks:", rec_tmp)
		else:
			self.dbid_price_asks = self.db_collection.insert_one({ 'rec-type': 'price.extr.asks', }).inserted_id
		print("TradeBook(dbInit)", self.wreq_prec, "id_bids:", self.dbid_price_bids, "id_asks:", self.dbid_price_asks)
		self.flag_db_init = True

	def upBookChan(self, chan_id):
		self.wrsp_chan = chan_id

	def upBookRec(self, rec_update):
		price_rec = rec_update[0]
		flag_bids = True if rec_update[2] >  0.0 else False
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		list_update = self.loc_book_bids if flag_bids else self.loc_book_asks
		idx_bgn = 0
		idx_end = len(list_update) - 1
		while idx_bgn < idx_end:
			idx_rec = int((idx_bgn + idx_end) / 2)
			mid_price = list_update[idx_rec][0]
			if   price_rec <  mid_price:
				idx_end = idx_rec - 1
			elif price_rec >  mid_price:
				idx_bgn = idx_rec + 1
			else:
				idx_bgn = idx_end = idx_rec
		idx_rec = idx_end
		# delete/add/update book record in self.loc_book_bids or self.loc_book_asks
		rec_del = rec_new = rec_up = False
		if   idx_rec >= 0 and rec_update[1] == 0:
			rec_del = True
			if list_update[idx_rec][0] == price_rec:
				del list_update[idx_rec]
		elif idx_rec <  0  or price_rec > list_update[idx_rec][0]:
			rec_new = True
			idx_rec = idx_rec + 1
			list_update.insert(idx_rec, rec_update)
		elif price_rec < list_update[idx_rec][0]:
			rec_new = True
			list_update.insert(idx_rec, rec_update)
		else:
			rec_up  = True
			list_update[idx_rec] = rec_update
		# update database
		price_min = None if len(list_update) == 0 else list_update[0][0]
		price_max = None if len(list_update) == 0 else list_update[len(list_update)-1][0]
		if flag_bids:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_bids, },
					{ '$set': { 'price-extr': [ price_min, price_max, ], }, })
		else:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_asks, },
					{ '$set': { 'price-extr': [ price_min, price_max, ], }, })
		str_rec_type = 'book.bids' if flag_bids else 'book.asks'
		if   rec_del:
			self.db_collection.delete_one({ 'rec-type': str_rec_type, 'book-price': price_rec, })
		elif rec_new:
			self.db_collection.insert_one({ 'rec-type': str_rec_type, 'book-price': price_rec,
						'book-count': rec_update[1], 'book-amount': rec_update[2], })
		elif rec_up:
			self.db_collection.find_one_and_update({ 'rec-type': str_rec_type, 'book-price': price_rec, },
						{ '$set': { 'book-count': rec_update[1], 'book-amount': rec_update[2], } })

	def upBookRecs(self, recs_update):
		if   type(recs_update[0]) is list:
			for rec_update in recs_update:
				self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Sn: ", json.dumps(self.loc_book_bids + self.loc_book_asks))
		else:
			rec_update = recs_update
			self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Up: ", json.dumps(rec_update), "=>", json.dumps(
						self.loc_book_bids if rec_update[2] > 0.0 else self.loc_book_asks))


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
		self.book_p0_book = TradeBook(db_database, "P0", 25)
		self.book_p1_book = TradeBook(db_database, "P1", 25)

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

