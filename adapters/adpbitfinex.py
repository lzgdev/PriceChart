import websocket
import _thread
import time

import hmac
import hashlib
import json

class TradeBook(object):
	def __init__(self, prec, size):
		self.flag_dbg  = True
		self.wreq_prec = prec
		self.wreq_len  = size
		self.wrsp_chan = None
		self.book_bids = []
		self.book_asks = []

	def upBookChan(self, chan_id):
		self.wrsp_chan = chan_id

	def upBookRec(self, rec_update):
		rec_price   = rec_update[0]
		list_update = self.book_bids if rec_update[2] >  0.0 else self.book_asks
		idx_bgn = 0
		idx_end = len(list_update) - 1
		while idx_bgn < idx_end:
			idx_ins = int((idx_bgn + idx_end) / 2)
			mid_price = list_update[idx_ins][0]
			if   rec_price <  mid_price:
				idx_end = idx_ins - 1
			elif rec_price >  mid_price:
				idx_bgn = idx_ins + 1
			else:
				idx_bgn = idx_end = idx_ins
		idx_rec = idx_end
		if   idx_rec >= 0 and rec_update[1] == 0:
			if list_update[idx_rec][0] == rec_price:
				del list_update[idx_rec]
		elif idx_rec <  0  or rec_price >  list_update[idx_rec][0]:
			list_update.insert(idx_rec+1, rec_update)
		elif rec_price < list_update[idx_rec][0]:
			list_update.insert(idx_rec, rec_update)
		else:
			list_update[idx_rec] = rec_update

	def upBookRecs(self, recs_update):
		if   type(recs_update[0]) is list:
			for rec_update in recs_update:
				self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Sn: ", json.dumps(self.book_bids + self.book_asks))
		else:
			rec_update = recs_update
			self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Up: ", json.dumps(rec_update), "=>", json.dumps(
						self.book_bids if rec_update[2] > 0.0 else self.book_asks))


class AdpBitfinexWSS(websocket.WebSocketApp):
	def __init__(self, url, api_key=None, api_secret=None):
		super(AdpBitfinexWSS, self).__init__(url)
		self.on_open = AdpBitfinexWSS.wssEV_OnOpen
		self.on_message = AdpBitfinexWSS.wssEV_OnMessage
		self.on_error = AdpBitfinexWSS.wssEV_OnError
		self.on_close = AdpBitfinexWSS.wssEV_OnClose
		self.auth_api_key = api_key
		self.auth_api_secret = api_secret
		self.book_p0_book = TradeBook("P0", 25)
		self.book_p1_book = TradeBook("P1", 25)

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

