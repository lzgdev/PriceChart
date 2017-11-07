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

	def	wssEV_OnOpen(self):
		if (self.auth_api_key != None) and (self.auth_api_secret != None):
			self.wss_auth(self.auth_api_key, self.auth_api_secret)
#		self.send(json.dumps(payload))
		self.send('{ "event": "subscribe", "channel": "ticker", "symbol": "tBTCUSD" }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len": 25 }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P1", "freq": "F0", "len": 25 }')
#		self.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len": 100 }')
#		run_times = 15
		run_times = 5
		def run(*args):
			for i in range(run_times):
				time.sleep(1)
				print("wait ...")
#				self.send('{ "event":"ping" }')
			time.sleep(1)
			self.close()
			print("thread terminating...")
		_thread.start_new_thread(run, ())

	def	wssEV_OnMessage(self, message):
		print(message)

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

