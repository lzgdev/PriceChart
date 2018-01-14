
import os

import websocket

import hmac
import hashlib
import json

from .dataset import DFMT_KKAIPRIV, DFMT_BITFINEX, MSEC_TIMEOFFSET

class CTNetClient_Base(websocket.WebSocketApp):
	def __init__(self, logger, url_ws):
		super(CTNetClient_Base, self).__init__(url_ws)
		self.logger   = logger
		self.pid_this = os.getpid()
		self.inf_this = 'WebSocket(pid=' + str(self.pid_this) + ')'
		self.on_open  = CTNetClient_Base.ncEV_Open
		self.on_message = CTNetClient_Base.ncEV_Message
		self.on_error = CTNetClient_Base.ncEV_Error
		self.on_close = CTNetClient_Base.ncEV_Close
		self.flag_log_intv = False

	def addObj_DataReceiver(self, obj_receiver, tok_channel):
		self.onNcOP_AddReceiver(obj_receiver, tok_channel)

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

	def onNcOP_AddReceiver(self, obj_receiver, tok_channel):
		pass

	def onNcOP_Exec_impl():
		pass

	def onNcEV_Open_impl(self):
		if self.flag_log_intv:
			self.logger.info(self.inf_this + " websocket Opened!")

	def onNcEV_Message_impl(self, message):
		pass

	def onNcEV_Error_impl(self, error):
		self.logger.error(self.inf_this + " websocket Error: " + str(error))

	def onNcEV_Close_impl(self):
		if self.flag_log_intv:
			self.logger.info(self.inf_this + " websocket Closed!")

class CTNetClient_WssBfx(CTNetClient_Base):
	def __init__(self, logger, tok_task, tok_this, url_ws, msec_off):
		super(CTNetClient_WssBfx, self).__init__(logger, url_ws)
		MSEC_TIMEOFFSET = msec_off
		self.tok_task = tok_task
		self.tok_this = tok_this
		self.objs_chan_data = []
		self.toks_chan_data = []
		self.flag_chan_actv = []
		self.num_chan_subscribed   = 0
		self.num_chan_unsubscribed = 0
		self.cntd_task_finish = -1
		self.flag_task_active = False
		self.flag_data_finish = False

	def ncOP_Send_Subscribe(self):
		for idx_chan, obj_chan in enumerate(self.objs_chan_data):
			obj_subscribe = {
					'event': 'subscribe',
					'channel': obj_chan.name_chan,
				}
			for wreq_key, wreq_value in obj_chan.wreq_args.items():
				obj_subscribe[wreq_key] = wreq_value
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)

	def ncOP_Send_Unsubscribe(self):
		for idx_chan, obj_chan in enumerate(self.objs_chan_data):
			if obj_chan.chan_id <= 0:
				continue
			obj_subscribe = {
					'event': 'unsubscribe',
					'chanId': obj_chan.chan_id,
				}
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)

	def onNcOP_AddReceiver(self, obj_receiver, tok_channel):
		if (obj_receiver != None):
			self.objs_chan_data.append(obj_receiver)
			self.toks_chan_data.append(tok_channel)
			self.flag_chan_actv.append(False)

	def onNcEV_Message_impl(self, message):
		#self.logger.info("CTNetClient_WssBfx(onNcEV_Message_impl): msg=" + message)
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
			self.logger.error(self.inf_this + " (mesg): can't handle msg=" + message)

	def onNcEV_Message_info(self, obj_msg):
		ver_msg  = obj_msg['version']
		code_msg = obj_msg['code'] if 'code' in obj_msg else None
		if ver_msg == 2 and code_msg == None:
			self.ncOP_Send_Subscribe()

	def onNcEV_Message_sbsc(self, evt_msg, obj_msg):
		idx_handler = -1
		cid_msg = obj_msg['chanId']
		flag_subscribed   = False
		flag_unsubscribed = False
		if   evt_msg == 'subscribed':
			for idx_chan, obj_chan in enumerate(self.objs_chan_data):
				if obj_chan.name_chan != obj_msg['channel']:
					continue
				idx_handler = idx_chan
				for wreq_key, wreq_value in obj_chan.wreq_args.items():
					if str(obj_msg[wreq_key]) != str(wreq_value):
						idx_handler = -1
				if idx_handler >= 0:
					break
			if idx_handler <  0:
				self.logger.error(self.inf_this + " (sbsc): can't handle subscribe, chanId=" +
								str(cid_msg) + ", obj=" + str(obj_msg))
			else:
				self.objs_chan_data[idx_handler].locSet_ChanId(cid_msg)
				if self.toks_chan_data[idx_handler].value <  self.tok_this:
					with self.toks_chan_data[idx_handler].get_lock():
						self.toks_chan_data[idx_handler].value = self.tok_this
				self.flag_chan_actv[idx_handler] =  True
				self.num_chan_subscribed += 1
				flag_subscribed   = True
				if self.flag_log_intv:
					self.logger.info(self.inf_this + " (sbsc): chan(idx=" +
								str(idx_handler) + ") subscribed, chanId=" + str(cid_msg))
		elif evt_msg == 'unsubscribed':
			for idx_chan, obj_chan in enumerate(self.objs_chan_data):
				if obj_chan.chan_id != cid_msg:
					continue
				idx_handler = idx_chan
			if idx_handler <  0:
				self.logger.error(self.inf_this + " (sbsc): can't handle unsubscribe, chanId=" + str(cid_msg) +
								", obj=" + str(obj_msg))
			else:
				self.objs_chan_data[idx_handler].locSet_ChanId(-1)
				self.flag_chan_actv[idx_handler] = False
				self.num_chan_unsubscribed += 1
				flag_unsubscribed = True
				if self.flag_log_intv:
					self.logger.info(self.inf_this + " (sbsc): chan(idx=" + str(idx_handler) +
							") unsubscribed, chanId=" + str(cid_msg))
		if   flag_subscribed and (
			self.num_chan_subscribed   == len(self.objs_chan_data)):
				if self.tok_task.value <  self.tok_this:
					with self.tok_task.get_lock():
						self.tok_task.value = self.tok_this
						self.flag_task_active = True
					#self.logger.info("CTNetClient_WssBfx(onNcEV_Message_sbsc): change token to " + str(self.tok_task))
		elif flag_unsubscribed and (
			self.num_chan_unsubscribed == len(self.objs_chan_data)):
			self.flag_data_finish = True

	def onNcEV_Message_data(self, obj_msg):
		idx_handler = -1
		cid_msg = obj_msg[0]
		for idx_chan in range(0, len(self.objs_chan_data)):
			if cid_msg == self.objs_chan_data[idx_chan].chan_id:
				idx_handler = idx_chan
				break
		if   idx_handler <  0:
			self.logger.error(self.inf_this + " (data): can't handle data, chanId:" + str(cid_msg) + ", data:" + str(obj_msg))
		elif not self.flag_chan_actv[idx_handler]:
			if self.flag_log_intv:
				self.logger.warning(self.inf_this + " (data): chan(idx=" + str(idx_handler) +
						") no longer active, ignore data chanId=" + str(cid_msg))
		else:
			#self.logger.debug(self.inf_this + "(data): chan(idx=" + str(idx_handler) + ") handle data, data=" + str(obj_msg))
			self.objs_chan_data[idx_handler].locDataAppend(DFMT_BITFINEX, obj_msg)

	def _run_kkai_step(self):
		#self.logger.warning(self.inf_this + "websocket KKAI Check: ...")
		for idx_chan in range(0, len(self.objs_chan_data)):
			if not self.flag_chan_actv[idx_chan]:
				continue
			if self.toks_chan_data[idx_chan].value != self.tok_this:
				self.flag_chan_actv[idx_chan] =  False
				if self.flag_log_intv:
					self.logger.warning(self.inf_this + " (check): chan=" + str(idx_chan) +
								" no longer active, unsubscribe.")
				self.send(json.dumps({ 'event': 'unsubscribe', 'chanId': self.objs_chan_data[idx_chan].chan_id, }))
		if self.flag_task_active:
			if self.tok_task.value != self.tok_this:
				self.flag_task_active = False
				self.cntd_task_finish = 10
		if not self.flag_data_finish and self.cntd_task_finish >= 0:
			self.cntd_task_finish -= 1
			if self.flag_log_intv:
				self.logger.warning(self.inf_this + " (check): chan=" + str(idx_chan) +
								" force finish count=" + str(self.cntd_task_finish) + " ...")
			if self.cntd_task_finish == 0:
				self.flag_data_finish =  True
				self.logger.warning(self.inf_this + " (check): chan=" + str(idx_chan) + " force finish.")
		return not self.flag_data_finish

