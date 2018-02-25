
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

		#self.flag_log_intv = 2
		self.flag_log_intv = 1

	def onPrep_Read_impl(self, **kwargs):
		self.list_chan_stat.clear()
		for cfg_chan in self.list_chan_cfg:
			if not cfg_chan.get('switch', True):
				continue
			map_chan  = self.obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
			stat_chan = { 'tok_chan': map_chan.get('tok_task', None), 'idx_chan': None,
								'channel': map_chan['channel'], 'wreq_args': map_chan['wreq_args'], 'dict_args': map_chan['dict_args'],
								'subsc_sent': None, 'subsc_recv': None, 'unsub_sent': None, 'unsub_recv': None,
								}
			if self.flag_log_intv >= 2:
				self.logger.debug(self.inf_this + " onPrep_Read_impl, stat_chan=" + str(stat_chan))
			self.list_chan_stat.append(stat_chan)

	def ncOP_Send_Subscribe(self):
		for stat_chan in self.list_chan_stat:
			obj_subscribe = {
					'event': 'subscribe',
					'channel': stat_chan['channel'],
				}
			obj_subscribe.update(stat_chan['dict_args'])
			if self.flag_log_intv >= 1:
				self.logger.info(self.inf_this + " ncOP_Send_Subscribe, chan=" + str(obj_subscribe))
			txt_wreq = json.dumps(obj_subscribe)
			self.send(txt_wreq)
			stat_chan['subsc_sent'] = self.obj_container.mtsNow_mono()

	def ncOP_Send_Unsubscribe(self):
		for tup_chan in self.obj_container.list_tups_datachan:
			if tup_chan[0].id_chan == None:
				continue
			self.ncOP_Send_Unsubscribe_chan(tup_chan[0].id_chan)

#	def ncOP_Send_Unsubscribe(self):
#		for tup_chan in self.obj_container.list_tups_datachan:
#			if tup_chan[0].id_chan == None:
#				continue
#			self.ncOP_Send_Unsubscribe_chan(tup_chan[0].id_chan)

	def ncOP_Send_Unsubscribe_chan(self, id_chan):
		idx_stat  = self._idxstat_of_idchan(id_chan)
		if idx_stat <  0:
			return -1
		obj_subscribe = {
					'event': 'unsubscribe',
					'chanId': int(id_chan),
			}
		if self.flag_log_intv >= 1:
			self.logger.info(self.inf_this + " ncOP_Send_Unsubscribe_chan, chan=" + str(obj_subscribe))
		txt_wreq = json.dumps(obj_subscribe)
		self.send(txt_wreq)
		self.list_chan_stat[idx_stat]['unsub_sent'] = self.obj_container.mtsNow_mono()
		return self.list_chan_stat[idx_stat]['idx_chan']

	def onNcEV_Message_impl(self, message):
		if self.flag_log_intv >= 2:
			self.logger.debug(self.inf_this + " onNcEV_Message_impl, msg=" + message)
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
		idx_stat  = -1
		idx_chan  = -1
		id_chan   = int(obj_msg['chanId'])
		name_chan = obj_msg['channel']
		# evaluate idx_stat
		for idx_stat_i, stat_chan in enumerate(self.list_chan_stat):
			if name_chan != stat_chan['channel']:
				continue
			idx_stat = idx_stat_i
			for chan_key, chan_val in stat_chan['dict_args'].items():
				if str(chan_val) == str(obj_msg.get(chan_key, None)):
					continue
				idx_stat = -1
				break
			if idx_stat >= 0:
				break
		if idx_stat <  0:
			return idx_chan
		# evaluate idx_chan
		idx_chan  = self.obj_container.datIN_ChanAdd(id_chan, name_chan, obj_msg)
		if idx_chan <  0:
			return idx_chan
		#print("onNcEV_Message_subscribed, idx_stat:", idx_stat, ", idx_chan:", idx_chan, obj_msg)
		# update stat members in self.list_chan_stat[idx_stat]
		tok_chan = self.list_chan_stat[idx_stat]['tok_chan']
		if tok_chan != None and tok_chan.value <  self.tok_mono_this:
			with tok_chan.get_lock():
				tok_chan.value = self.tok_mono_this
			if self.flag_log_intv >= 1:
				self.logger.info(self.inf_this + " subscribed: chanId=" + str(id_chan) +
								", tok=" + str(tok_chan.value) + ", chan=" + name_chan)
		self.list_chan_stat[idx_stat]['idx_chan'] = idx_chan
		self.list_chan_stat[idx_stat]['subsc_recv'] = self.obj_container.mtsNow_mono()
		if self.flag_log_intv >= 1:
			self.logger.debug(self.inf_this + " onNcEV_Message_subscribed, stat_chan=" + str(self.list_chan_stat[idx_stat]))
		return idx_chan

	def onNcEV_Message_unsubscribed(self, obj_msg):
		id_chan  = obj_msg['chanId']
		idx_stat = self._idxstat_of_idchan(id_chan)
		if idx_stat <  0:
			return -1
		tok_chan = self.list_chan_stat[idx_stat]['tok_chan']
		if tok_chan != None:
			if self.flag_log_intv >= 1:
				self.logger.info(self.inf_this + " unsubscribed: chanId=" + str(id_chan) +
								", tok=" + str(tok_chan.value))
		self.list_chan_stat[idx_stat]['unsub_recv'] = self.obj_container.mtsNow_mono()
		self.obj_container.datIN_ChanDel(id_chan)
		return self.list_chan_stat[idx_stat]['idx_chan']

	def onNcEV_Message_data(self, obj_msg):
		id_chan  = obj_msg[0]
		idx_stat = self._idxstat_of_idchan(id_chan)
		if idx_stat <  0:
			return -1
		tok_chan = self.list_chan_stat[idx_stat]['tok_chan']
		if tok_chan == None or self.tok_mono_this != tok_chan.value:
			self.onNcEV_TokOut(idx_stat)
		else:
			if self.flag_log_intv >= 3:
				self.logger.info(self.inf_this + " onNcEV_Message_data, tok=" + str(self.tok_mono_this) +
								" match new=" + str(tok_chan.value))
			self.obj_container.datIN_DataFwd(id_chan, DFMT_BFXV2, obj_msg)

	def onNcEV_TokOut(self, idx_stat):
		stat_chan = self.list_chan_stat[idx_stat]
		tok_chan  = stat_chan['tok_chan']
		idx_chan  = stat_chan['idx_chan']
		id_chan   = self.obj_container.list_tups_datachan[idx_chan][0].id_chan
		if id_chan == None:
			return False
		mono_sent = stat_chan['unsub_sent']
		mono_now  = self.obj_container.mtsNow_mono()
		if mono_sent == None or mono_now >= (mono_sent + 1000):
			if self.flag_log_intv >= 1:
				self.logger.warning(self.inf_this + " chanId=" + str(id_chan) + " tok=" + str(self.tok_mono_this) +
								" NOT match new=" + str(tok_chan.value))
			self.ncOP_Send_Unsubscribe_chan(id_chan)

	# private utility methods
	def _idxstat_of_idchan(self, id_chan):
		idx_stat = -1
		if id_chan == None:
			return idx_stat
		for idx_stat_i, stat_chan in enumerate(self.list_chan_stat):
			idx_chan = stat_chan['idx_chan']
			if idx_chan == None:
				continue
			if id_chan  != self.obj_container.list_tups_datachan[idx_chan][0].id_chan:
				continue
			idx_stat = idx_stat_i
			break
		return idx_stat

	def _run_kkai_step(self):
		if self.flag_log_intv >= 4:
			self.logger.warning(self.inf_this + " websocket KKAI Check: " + str(self.list_chan_stat))
		num_chans   = 0
		num_finish  = 0
		num_timeout = 0
		for idx_stat, stat_chan in enumerate(self.list_chan_stat):
			num_chans += 1
			if stat_chan['unsub_recv'] != None:
				num_finish += 1
			if stat_chan['subsc_recv'] == None:
				continue
			tok_chan   = stat_chan['tok_chan']
			if tok_chan == None or self.tok_mono_this != tok_chan.value:
				self.onNcEV_TokOut(idx_stat)
		flag_finish =  True if (num_finish + num_timeout) >= num_chans else False
		return not flag_finish


