import websocket
import _thread
import time

import hmac
import hashlib
import json

class CTNetClient_Base(websocket.WebSocketApp):
	def __init__(self, logger, url_ws):
		super(CTNetClient_Base, self).__init__(url_ws)
		self.logger   = logger
		self.on_open  = CTNetClient_Base.ncEV_Open
		self.on_message = CTNetClient_Base.ncEV_Message
		self.on_error = CTNetClient_Base.ncEV_Error
		self.on_close = CTNetClient_Base.ncEV_Close

	def addObj_DataReceiver(self, obj_receiver):
		self.onNcOP_AddReceiver(obj_receiver)

	def ncOP_Exec():
		self.onNcOP_Exec_impl()

	def ncEV_Open(self):
		self.onNcEV_Open_impl()

	def ncEV_Message(self, message):
		self.onNcEV_Message_impl(message)

	def ncEV_Error(self, error):
		self.onNcEV_Error_impl(error)

	def ncEV_Close(self):
		self.onNcEV_Close_impl()

	def onNcOP_AddReceiver(obj_receiver):
		pass

	def onNcOP_Exec_impl():
		pass

	def onNcEV_Open_impl(self):
		pass

	def onNcEV_Message_impl(self, message):
		pass

	def onNcEV_Error_impl(self, error):
		self.logger.error('WebSocket Error: ' + str(error))

	def onNcEV_Close_impl(self):
		pass

class CTNetClient_BfxWss(CTNetClient_Base):
	def __init__(self, logger, url_ws):
		super(CTNetClient_BfxWss, self).__init__(logger, url_ws)
		self.objs_chan_data = []

	def onNcOP_AddReceiver(self, obj_receiver):
		if (obj_receiver != None):
			self.objs_chan_data.append(obj_receiver)

	def onNcEV_Open_impl(self):
		self.logger.info('WebSocket connected!')
		run_times = 15
		def run(*args):
			for i in range(run_times):
				time.sleep(1)
				self.logger.info('wait ...')
			time.sleep(1)
			self.close()
			self.logger.info('thread terminating...')
		_thread.start_new_thread(run, ())

	def onNcEV_Message_impl(self, message):
		obj_msg  = None
		if isinstance(message, str):
			obj_msg  = json.loads(message)
		if isinstance(obj_msg, dict) and (obj_msg['event'] == 'info'):
			self.onNcEV_Message_info(obj_msg)
		elif obj_msg != None:
			self.onNcEV_Message_data(obj_msg)

	def onNcEV_Message_info(self, obj_msg):
		for chan_idx, obj_chan in enumerate(self.objs_chan_data):
			obj_subscribe = {
					'event': 'subscribe',
					'channel': obj_chan.name_chan,
				}
			for wreq_key, wreq_value in obj_chan.wreq_args.items():
				obj_subscribe[wreq_key] = wreq_value
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)

	def onNcEV_Message_data(self, obj_msg):
		if isinstance(obj_msg, list):
			handler_msg = None
			cid_msg = obj_msg[0]
			for chan_idx, obj_chan in enumerate(self.objs_chan_data):
				if cid_msg == obj_chan.chan_id:
					handler_msg = obj_chan
					break
			if handler_msg == None:
				self.logger.error("CTNetClient_BfxWss(onNcEV_Message_data): can't handle data, chanId:" + str(cid_msg) + ", obj:" + str(obj_msg))
			else:
				#self.logger.debug("CTNetClient_BfxWss(onNcEV_Message_data): handle data, chanId:" + str(cid_msg) + ", obj:" + str(obj_msg))
				handler_msg.locAppendData(2001, obj_msg)
		elif isinstance(obj_msg, dict) and obj_msg['event'] == 'subscribed':
			handler_msg = None
			cid_msg = obj_msg['chanId']
			for chan_idx, obj_chan in enumerate(self.objs_chan_data):
				if obj_chan.name_chan != obj_msg['channel']:
					continue
				handler_msg = obj_chan
				for wreq_key, wreq_value in obj_chan.wreq_args.items():
					if obj_msg[wreq_key] != str(wreq_value):
						handler_msg = None
						break
			if handler_msg == None:
				self.logger.error("CTNetClient_BfxWss(onNcEV_Message_data): can't handle subscribe, chanId:" + str(cid_msg) + ", obj:" + str(obj_msg))
			else:
				handler_msg.locSet_ChanId(cid_msg)
		else:
			self.logger.error("CTNetClient_BfxWss(onNcEV_Message_data): can't handle obj=" + str(obj_msg))

"""
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
"""

