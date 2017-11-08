import websocket
import _thread
import time

import hmac
import hashlib
import json

class AdpBitfinexWSS(websocket.WebSocketApp):

	def __init__(self, url, api_key=None, api_secret=None):
		super(AdpBitfinexWSS, self).__init__(url)
		self.on_open = AdpBitfinexWSS.wssEV_OnOpen
		self.on_message = AdpBitfinexWSS.wssEV_OnMessage
		self.on_error = AdpBitfinexWSS.wssEV_OnError
		self.on_close = AdpBitfinexWSS.wssEV_OnClose
		self.auth_api_key = api_key
		self.auth_api_secret = api_secret
		self.book_r0_chanid  = None
		self.book_p0_chanid  = None
		self.book_p1_chanid  = None
		self.book_p2_chanid  = None
		self.book_p3_chanid  = None

	def	wssEV_OnOpen(self):
		if (self.auth_api_key != None) and (self.auth_api_secret != None):
			self.wss_auth(self.auth_api_key, self.auth_api_secret)
#		self.send('{ "event": "subscribe", "channel": "ticker", "symbol": "tBTCUSD" }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "R0", "len": 100 }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len":  25 }')
		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len": 100 }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P1", "freq": "F0", "len":  25 }')
#		run_times = 15
		run_times =  8
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
			if not type(rsp_list[1]) is list:
				print("msg(misc): ", message)
			elif self.book_p0_chanid == rsp_list[0]:
				print("msg(book p0): len=", len(rsp_list), "list:", message)
			elif self.book_p1_chanid == rsp_list[0]:
				print("msg(book p1): len=", len(rsp_list), "list:", message)
			elif self.book_r0_chanid == rsp_list[0]:
				print("msg(book r0): len=", len(rsp_list), "list:", message)
			else:
				print("msg(list): ", message)
		elif not rsp_dict is None and rsp_dict['event'] == 'subscribed':
			if   rsp_dict['channel'] == 'book' and rsp_dict['prec'] == 'P0':
				self.book_p0_chanid  = rsp_dict['chanId']
			elif rsp_dict['channel'] == 'book' and rsp_dict['prec'] == 'P1':
				self.book_p1_chanid  = rsp_dict['chanId']
			elif   rsp_dict['channel'] == 'book' and rsp_dict['prec'] == 'R0':
				self.book_r0_chanid = rsp_dict['chanId']
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

