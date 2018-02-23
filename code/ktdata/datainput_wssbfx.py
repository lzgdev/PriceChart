
import hmac
import hashlib
import json

from .datainput         import CTDataInput_Ws
from .dataset           import DFMT_KKAIPRIV, DFMT_BFXV2, MSEC_TIMEOFFSET


class CTDataInput_WssBfx(CTDataInput_Ws):
	def __init__(self, logger, obj_container, url_ws, tok_this):
		CTDataInput_Ws.__init__(self, logger, obj_container, url_ws)
		self.tok_mono_this   = tok_this
		self.list_chan_stat  = [ ]

		self.num_chan_all    = 0
		self.num_subsc_sent  = 0
		self.num_subsc_recv  = 0
		self.num_unsub_sent  = 0
		self.num_unsub_recv  = 0

		self.num_chan_subscribed   = 0
		self.num_chan_unsubscribed = 0
		self.cntd_task_finish = -1
		self.flag_task_active = False
		self.flag_data_finish = False

		self.inf_this = 'DinWsBfx(pid=' + str(self.pid_this) + ',tok=' + str(self.tok_mono_this) + ')'

	def onPrep_Read_impl(self, **kwargs):
		self.list_chan_stat.clear()
		num_chans = len(self.obj_container.list_tups_datachan)
		for idx_chan in range(0, num_chans):
			tsk_chan = self.obj_container._gmap_TaskChans_chan(self.obj_container.list_tups_datachan[idx_chan][2],
								self.obj_container.list_tups_datachan[idx_chan][4])
			self.list_chan_stat.append({ 'tok_chan': tsk_chan.get('tok_task', None),
								'subsc_sent': None, 'subsc_recv': None, 'unsub_sent': None, 'unsub_recv': None, })
		#print("CTDataInput_WssBfx::onPrep_Read_impl:", str(self.list_chan_stat))


	def ncOP_Send_Subscribe(self):
		num_chans = len(self.obj_container.list_tups_datachan)
		for idx_chan in range(num_chans):
			tup_chan = self.obj_container.list_tups_datachan[idx_chan]
			if tup_chan[0].id_chan != None:
				continue
			obj_subscribe = {
					'event': 'subscribe',
					'channel': tup_chan[2],
				}
			if tup_chan[4] != None:
				obj_subscribe.update(tup_chan[4])
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)
			self.list_chan_stat[idx_chan]['subsc_sent'] = self.obj_container.mtsNow_mono()

	def ncOP_Send_Unsubscribe(self):
		for tup_chan in self.obj_container.list_tups_datachan:
			if tup_chan[0].id_chan == None:
				continue
			self.ncOP_Send_Unsubscribe_chan(tup_chan[0].id_chan)

	def ncOP_Send_Unsubscribe_chan(self, id_chan):
		idx_chan  = self._idx_of_idchan(id_chan)
		if idx_chan <  0:
			return idx_chan
		obj_subscribe = {
					'event': 'unsubscribe',
					'chanId': int(id_chan),
			}
		txt_wreq = json.dumps(obj_subscribe)
		self.send(txt_wreq)
		self.list_chan_stat[idx_chan]['unsub_sent'] = self.obj_container.mtsNow_mono()

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
				self.onNcEV_Message_subscribed(obj_msg)
			elif (evt_msg == 'unsubscribed'):
				self.onNcEV_Message_unsubscribed(obj_msg)
		else:
			self.logger.error(self.inf_this + " (mesg): can't handle msg=" + message)

	def onNcEV_Message_info(self, obj_msg):
		ver_msg  = obj_msg.get('version', None)
		code_msg = obj_msg.get('code', None)
		if ver_msg == 2 and code_msg == None:
			self.ncOP_Send_Subscribe()

	def onNcEV_Message_subscribed(self, obj_msg):
		id_chan   = int(obj_msg['chanId'])
		name_chan = obj_msg['channel']
		idx_chan  = self.obj_container.datIN_ChanAdd(id_chan, name_chan, obj_msg)
		if idx_chan <  0:
			return idx_chan
		# update stat members in self.list_chan_stat[idx_chan]
		tok_chan = self.list_chan_stat[idx_chan]['tok_chan']
		if tok_chan != None and tok_chan.value <  self.tok_mono_this:
			with tok_chan.get_lock():
				tok_chan.value = self.tok_mono_this
			self.logger.info(self.inf_this + " subscribed: chanId=" + str(id_chan) +
						", chan=" + name_chan + ", tok=" + str(tok_chan.value))
		self.list_chan_stat[idx_chan]['subsc_recv'] = self.obj_container.mtsNow_mono()
		return idx_chan

	def onNcEV_Message_unsubscribed(self, obj_msg):
		id_chan  = obj_msg['chanId']
		idx_chan = self._idx_of_idchan(id_chan)
		if idx_chan <  0:
			return idx_chan
		tok_chan = self.list_chan_stat[idx_chan]['tok_chan']
		if tok_chan != None:
			self.logger.info(self.inf_this + " unsubscribed: chanId=" + str(id_chan) +
						", tok=" + str(tok_chan.value))
		self.list_chan_stat[idx_chan]['unsub_recv'] = self.obj_container.mtsNow_mono()
		self.obj_container.datIN_ChanDel(id_chan)
		return idx_chan

	def onNcEV_Message_data(self, obj_msg):
		id_chan  = obj_msg[0]
		idx_chan = self._idx_of_idchan(id_chan)
		if idx_chan <  0:
			return idx_chan
		tok_chan = self.list_chan_stat[idx_chan]['tok_chan']
		if tok_chan == None:
			pass
		elif self.tok_mono_this == tok_chan.value:
			#self.logger.info(self.inf_this + " tok=" + str(self.tok_mono_this) + " match new=" + str(tok_chan.value))
			self.obj_container.datIN_DataFwd(id_chan, DFMT_BFXV2, obj_msg)
		else:
			self.logger.warning(self.inf_this + " chanId=" + str(id_chan) + " tok=" + str(self.tok_mono_this) +
						" NOT match new=" + str(tok_chan.value))
			if self.list_chan_stat[idx_chan]['unsub_sent'] == None:
				self.ncOP_Send_Unsubscribe_chan(id_chan)

	# private utility methods
	def _idx_of_idchan(self, id_chan):
		idx_chan  = -1
		if id_chan == None:
			return idx_chan
		num_chans = len(self.obj_container.list_tups_datachan)
		for idx_chan_it in range(num_chans):
			if self.obj_container.list_tups_datachan[idx_chan_it][0].id_chan != id_chan:
				continue
			idx_chan  = idx_chan_it
			break
		return idx_chan

	def _run_kkai_step(self):
		#self.logger.warning(self.inf_this + " websocket KKAI Check: " + str(self.list_chan_stat))
		num_chans   = len(self.list_chan_stat)
		num_finish  = 0
		num_timeout = 0
		for idx_chan in range(num_chans):
			if self.list_chan_stat[idx_chan]['unsub_recv'] != None:
				num_finish += 1
		flag_finish =  True if (num_finish + num_timeout) >= num_chans else False
		"""
		# __init__ b
		self.objs_chan_data = []
		self.toks_chan_data = []
		self.flag_chan_actv = []
		# __init__ e
		# onNcOP_AddReceiver b (self, obj_receiver, tok_channel):
		if (obj_receiver != None):
			self.objs_chan_data.append(obj_receiver)
			self.toks_chan_data.append(tok_channel)
			self.flag_chan_actv.append(False)
		# onNcOP_AddReceiver e

		for idx_chan in range(0, len(self.objs_chan_data)):
			if not self.flag_chan_actv[idx_chan]:
				continue
			if self.toks_chan_data[idx_chan].value != self.tok_this:
				self.flag_chan_actv[idx_chan] =  False
				if self.flag_log_intv:
					self.logger.warning(self.inf_this + " (check): chan=" + str(idx_chan) +
								" no longer active, unsubscribe.")
				self.send(json.dumps({ 'event': 'unsubscribe', 'chanId': self.objs_chan_data[idx_chan].id_chan, }))
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
		"""
		return not flag_finish


