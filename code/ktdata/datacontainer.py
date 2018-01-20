
import os

import hmac
import hashlib
import json

from .datainput     import CTDataInput_Ws

from .dataset       import DFMT_KKAIPRIV, DFMT_BITFINEX, MSEC_TIMEOFFSET
from .dataset       import CTDataSet_Ticker, CTDataSet_ATrades, CTDataSet_ABooks, CTDataSet_ACandles

class CTDataContainer(object):
	def __init__(self, logger):
		object.__init__(self)
		self.logger   = logger
		self.pid_this = os.getpid()
		self.list_objs_datasrc = []
		self.list_tups_datachn = []

	def execLoop(self):
		self.onExec_Loop_impl()

	def addArg_DataChannel(self, name_chan, dict_args, tmp_tokchan):
		obj_netclient = self.list_objs_datasrc[0]
		#{ 'channel':  'ticker', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', }, },

		num_coll_msec  =  3 * 60 * 60 * 1000
		#num_coll_msec  =  1 * 60 * 60 * 1000

		obj_chan  = None
		#self.logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))
		if   name_chan == 'ticker':
			obj_chan = CTDataSet_Ticker(self.logger, self, dict_args)
		elif name_chan == 'trades':
			obj_chan = CTDataSet_ATrades(512, self.logger, self, dict_args)
		elif name_chan == 'book':
			obj_chan = CTDataSet_ABooks(self.logger, self, dict_args)
		elif name_chan == 'candles':
			obj_chan = CTDataSet_ACandles(512, self.logger, self, dict_args)

		if obj_chan != None:
			self.list_tups_datachn.append((obj_chan, name_chan, dict_args))

	def addObj_DataSource(self, obj_source):
		self.list_objs_datasrc.append(obj_source)

	def datCHK_IsFinish(self):
		return self.onDatCHK_IsFinish_impl()

	def datIN_ChanAdd(self, id_chan, name_chan, dict_msg):
		idx_chan = self.onDatIN_ChanAdd_impl(id_chan, name_chan, dict_msg)
		if idx_chan >= 0:
			self.onDatIN_ChanAdd_ext(idx_chan, id_chan, name_chan, dict_msg)
		return idx_chan

	def datIN_ChanDel(self, id_chan, name_chan, dict_msg):
		idx_chan = self.onDatIN_ChanDel_impl(id_chan, name_chan, dict_msg)
		if idx_chan >= 0:
			self.onDatIN_ChanDel_ext(idx_chan, id_chan, name_chan, dict_msg)
		return idx_chan

	def datIN_DataFwd(self, id_chan, fmt_data, obj_msg):
		self.onDatIN_DataFwd_impl(id_chan, fmt_data, obj_msg)

	def datCB_DataClean(self, obj_dataset):
		idx_chan = self.__priv_Dset2Idx(obj_dataset)
		if idx_chan >= 0:
			self.onDatCB_DataClean_impl(idx_chan, obj_dataset)

	def datCB_DataSync(self, obj_dataset, msec_now):
		idx_chan = self.__priv_Dset2Idx(obj_dataset)
		if idx_chan >= 0:
			self.onDatCB_DataSync_impl(idx_chan, obj_dataset, msec_now)

	def datCB_RecPlus(self, obj_dataset, doc_rec, idx_rec):
		idx_chan = self.__priv_Dset2Idx(obj_dataset)
		if idx_chan >= 0:
			self.onDatCB_RecPlus_impl(idx_chan, obj_dataset, doc_rec, idx_rec)

	def onExec_Loop_impl(self):
		for obj_source in self.list_objs_datasrc:
			if isinstance(obj_source, CTDataInput_Ws):
				obj_source.run_forever()

	def onDatCHK_IsFinish_impl(self):
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
		return False

	def onDatIN_ChanAdd_impl(self, id_chan, name_chan, dict_msg):
		#print("CTDataContainer::onDatIN_ChanAdd_impl() ", id_chan, name_chan, dict_msg)
		idx_chan_add = -1
		for idx_chan in range(len(self.list_tups_datachn)):
			tup_chan = self.list_tups_datachn[idx_chan]
			if tup_chan[0].id_chan != None:
				continue
			if tup_chan[1] != name_chan:
				continue
			idx_chan_add = idx_chan
			for key, val in tup_chan[2].items():
				if str(val) != str(dict_msg[key]):
					idx_chan_add = -1
					break
			if idx_chan_add >= 0:
				break
		if idx_chan_add <  0:
			self.logger.error(self.inf_this + " (sbsc): can't handle subscribe, chanId=" +
								str(cid_msg) + ", obj=" + str(obj_msg))
		else:
			self.list_tups_datachn[idx_chan_add][0].locSet_ChanId(id_chan)
			"""
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
			"""
		return idx_chan_add

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		pass

	def onDatIN_ChanDel_impl(self, id_chan, name_chan, dict_msg):
		"""
		flag_subscribed   = False
		flag_unsubscribed = False

		for idx_chan, obj_chan in enumerate(self.objs_chan_data):
			if obj_chan.id_chan != cid_msg:
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
					#self.logger.info("CTDataInput_WssBfx(onNcEV_Message_sbsc): change token to " + str(self.tok_task))
		elif flag_unsubscribed and (
			self.num_chan_unsubscribed == len(self.objs_chan_data)):
			self.flag_data_finish = True
		"""
		pass

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		pass

	def onDatIN_DataFwd_impl(self, id_chan, fmt_data, obj_msg):
		#print("CTDataContainer::onDatIN_DataFwd_impl() ", id_chan, fmt_data, obj_msg)
		obj_chan = None
		for idx_chan in range(len(self.list_tups_datachn)):
			tup_chan = self.list_tups_datachn[idx_chan]
			if tup_chan[0].id_chan == id_chan:
				obj_chan = tup_chan[0]
				break
		if obj_chan == None:
			self.logger.error(self.inf_this + " (data): can't handle data, chanId:" + str(cid_msg) + ", data:" + str(obj_msg))
		else:
			obj_chan.locDataAppend(fmt_data, obj_msg)
		"""
		idx_handler = -1
		cid_msg = obj_msg[0]
		for idx_chan in range(0, len(self.objs_chan_data)):
			if cid_msg == self.objs_chan_data[idx_chan].id_chan:
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
		"""
		pass

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		pass

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		pass


	def __priv_Dset2Idx(self, obj_dataset):
		idx_chan_this = -1
		for idx_chan in range(0, len(self.list_tups_datachn)):
			if obj_dataset == self.list_tups_datachn[idx_chan][0]:
				idx_chan_this = idx_chan
				break
		return idx_chan_this


