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


