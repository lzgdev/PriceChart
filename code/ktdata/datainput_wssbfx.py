
import hmac
import hashlib
import json

from .datainput import CTDataInput_Ws

from .dataset   import DFMT_KKAIPRIV, DFMT_BITFINEX, MSEC_TIMEOFFSET


class CTDataInput_WssBfx(CTDataInput_Ws):
	def __init__(self, logger, obj_container, tok_task, tok_this, url_ws, msec_off):
		CTDataInput_Ws.__init__(self, logger, obj_container, url_ws)
		MSEC_TIMEOFFSET = msec_off
		self.tok_task = tok_task
		self.tok_this = tok_this
		self.num_chan_subscribed   = 0
		self.num_chan_unsubscribed = 0
		self.cntd_task_finish = -1
		self.flag_task_active = False
		self.flag_data_finish = False

	def ncOP_Send_Subscribe(self):
		for tup_chan in self.obj_container.list_tups_datachn:
			if tup_chan[0].id_chan != None:
				continue
			obj_subscribe = {
					'event': 'subscribe',
					'channel': tup_chan[1],
				}
			obj_subscribe.update(tup_chan[2])
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)

	def ncOP_Send_Unsubscribe(self):
		for tup_chan in self.obj_container.list_tups_datachn:
			if tup_chan[0].id_chan == None:
				continue
			obj_subscribe = {
					'event': 'unsubscribe',
					'chanId': tup_chan[0].id_chan,
				}
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)

	def onNcEV_Message_impl(self, message):
		#self.logger.info("CTDataInput_WssBfx(onNcEV_Message_impl): msg=" + message)
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
			elif (evt_msg == 'subscribed'):
				self.obj_container.datIN_ChanAdd(obj_msg['chanId'], obj_msg['channel'], obj_msg)
			elif (evt_msg == 'unsubscribed'):
				self.obj_container.datIN_ChanDel(obj_msg['chanId'], obj_msg['channel'], obj_msg)
		else:
			self.logger.error(self.inf_this + " (mesg): can't handle msg=" + message)

	def onNcEV_Message_info(self, obj_msg):
		ver_msg  = obj_msg['version']
		code_msg = obj_msg['code'] if 'code' in obj_msg else None
		if ver_msg == 2 and code_msg == None:
			self.ncOP_Send_Subscribe()

	def onNcEV_Message_data(self, obj_msg):
		self.obj_container.datIN_DataFwd(obj_msg[0], DFMT_BITFINEX, obj_msg)

	def _run_kkai_step(self):
		#self.logger.warning(self.inf_this + "websocket KKAI Check: ...")
		return not self.obj_container.datCHK_IsFinish()

