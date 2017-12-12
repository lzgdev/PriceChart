import websocket

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
		self.logger.info("WebSocket Opened!")

	def onNcEV_Message_impl(self, message):
		pass

	def onNcEV_Error_impl(self, error):
		self.logger.error("WebSocket Error: " + str(error))

	def onNcEV_Close_impl(self):
		self.logger.warning("WebSocket Closed!")

class CTNetClient_BfxWss(CTNetClient_Base):
	def __init__(self, logger, url_ws):
		super(CTNetClient_BfxWss, self).__init__(logger, url_ws)
		self.objs_chan_data = []

		self.dbg_count = 0

	def onNcOP_AddReceiver(self, obj_receiver):
		if (obj_receiver != None):
			self.objs_chan_data.append(obj_receiver)

	def onNcEV_Message_impl(self, message):
		#self.logger.info("CTNetClient_BfxWss(onNcEV_Message_impl): msg=" + message)
		if not isinstance(message, str):
			obj_msg  = None
		else:
			try:
				obj_msg  = json.loads(message)
			except:
				obj_msg  = None
		if   isinstance(obj_msg, list):
			self.onNcEV_Message_data(obj_msg)
		elif isinstance(obj_msg, dict):
			evt_msg = obj_msg['event']
			if   (evt_msg == 'info'):
				self.onNcEV_Message_info(obj_msg)
			elif (evt_msg == 'subscribed') or (evt_msg == 'unsubscribed'):
				self.onNcEV_Message_sbsc(evt_msg, obj_msg)
		else:
			self.logger.error("CTNetClient_BfxWss(onNcEV_Message_impl): can't handle msg=" + message)

	def onNcEV_Message_info(self, obj_msg):
		ver_msg  = obj_msg['version']
		code_msg = obj_msg['code'] if 'code' in obj_msg else None
		if ver_msg == 2 and code_msg == None:
			for idx_chan, obj_chan in enumerate(self.objs_chan_data):
				obj_subscribe = {
						'event': 'subscribe',
						'channel': obj_chan.name_chan,
					}
				for wreq_key, wreq_value in obj_chan.wreq_args.items():
					obj_subscribe[wreq_key] = wreq_value
				txt_wreq = json.dumps(obj_subscribe)
				self.send(txt_wreq)

	def onNcEV_Message_sbsc(self, evt_msg, obj_msg):
		handler_msg = None
		cid_msg = obj_msg['chanId']
		if   evt_msg == 'subscribed':
			for idx_chan, obj_chan in enumerate(self.objs_chan_data):
				if obj_chan.name_chan != obj_msg['channel']:
					continue
				handler_msg = obj_chan
				for wreq_key, wreq_value in obj_chan.wreq_args.items():
					if obj_msg[wreq_key] != str(wreq_value):
						handler_msg = None
						break
			if handler_msg != None:
				handler_msg.locSet_ChanId(cid_msg)
			else:
				self.logger.error("CTNetClient_BfxWss(onNcEV_Message_sbsc): can't handle subscribe, chanId:" +
								str(cid_msg) + ", obj:" + str(obj_msg))
		elif evt_msg == 'unsubscribed':
			self.logger.error("CTNetClient_BfxWss(onNcEV_Message_sbsc): unsubscribed chanId:" +
								str(cid_msg) + ", obj:" + str(obj_msg))

	def onNcEV_Message_data(self, obj_msg):
		flag_term = False
		handler_msg = None
		cid_msg = obj_msg[0]
		for idx_chan, obj_chan in enumerate(self.objs_chan_data):
			if cid_msg == obj_chan.chan_id:
				handler_msg = obj_chan
				break
		if handler_msg == None:
			self.logger.error("CTNetClient_BfxWss(onNcEV_Message_data): can't handle data, chanId:" + str(cid_msg) + ", obj:" + str(obj_msg))
		else:
			#self.logger.debug("CTNetClient_BfxWss(onNcEV_Message_data): handle data, chanId:" + str(cid_msg) + ", obj:" + str(obj_msg))
			handler_msg.locAppendData(2001, obj_msg)
			flag_term = handler_msg.flag_loc_term
		if flag_term:
			self.logger.info("CTNetClient_BfxWss(onNcEV_Message_data): unsubscribe data, chanId:" + str(cid_msg))
			self.send(json.dumps({ 'event': 'unsubscribe', 'chanId': cid_msg, }))

	def _run_kkai_step(self):
		self.logger.warning("WebSocket KKAI Check: count=" + str(self.dbg_count))
		self.dbg_count += 1
		return False if (self.dbg_count >  20) else True

