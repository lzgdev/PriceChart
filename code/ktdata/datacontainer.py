
import os

import hmac
import hashlib
import json

from .dataset       import DFMT_KKAIPRIV, DFMT_BFXV2, MSEC_TIMEOFFSET
from .dataset       import CTDataSet_Ticker, CTDataSet_ATrades, CTDataSet_ABooks, CTDataSet_ACandles

gMap_TaskChans_init = False

gMap_TaskChans = [
		# channel:       0 for ticker
		{ 'channel':  'ticker', 'name_dbtbl':        'ticker-tBTCUSD', 'wreq_args':  '{ "symbol": "tBTCUSD" }', },
		# channel:       1 for trades
		{ 'channel':  'trades', 'name_dbtbl':        'trades-tBTCUSD', 'wreq_args':  '{ "symbol": "tBTCUSD" }', },
		# channel:  2 ~  5 for book
		{ 'channel':    'book', 'name_dbtbl':       'book-tBTCUSD-P0', 'wreq_args':  '{ "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'name_dbtbl':       'book-tBTCUSD-P1', 'wreq_args':  '{ "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'name_dbtbl':       'book-tBTCUSD-P2', 'wreq_args':  '{ "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'name_dbtbl':       'book-tBTCUSD-P3', 'wreq_args':  '{ "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" }', },
		# channel:  6 ~ 17 for book
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-1m', 'wreq_args':  '{ "key": "trade:1m:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-5m', 'wreq_args':  '{ "key": "trade:5m:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':   'candles-tBTCUSD-15m', 'wreq_args': '{ "key": "trade:15m:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':   'candles-tBTCUSD-30m', 'wreq_args': '{ "key": "trade:30m:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-1h', 'wreq_args':  '{ "key": "trade:1h:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-3h', 'wreq_args':  '{ "key": "trade:3h:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-6h', 'wreq_args':  '{ "key": "trade:6h:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':   'candles-tBTCUSD-12h', 'wreq_args': '{ "key": "trade:12h:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-1D', 'wreq_args':  '{ "key": "trade:1D:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-7D', 'wreq_args':  '{ "key": "trade:7D:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':   'candles-tBTCUSD-14D', 'wreq_args': '{ "key": "trade:14D:tBTCUSD" }', },
		{ 'channel': 'candles', 'name_dbtbl':    'candles-tBTCUSD-1M', 'wreq_args':  '{ "key": "trade:1M:tBTCUSD" }', },
	]


class CTDataContainer(object):
	def __init__(self, logger):
		global gMap_TaskChans_init
		object.__init__(self)
		self.logger   = logger
		self.inf_this = "DataContainer"
		self.pid_this = os.getpid()
		self.list_tups_datasrc  = []
		self.list_tups_datachan = []
		# init global chans map table
		if not gMap_TaskChans_init:
			gMap_TaskChans_init = self._gmap_TaskChans_init()

	def execMain(self, **kwargs):
		ret_init = self.onExec_Init_impl(**kwargs)
		if None != ret_init:
			return ret_init
		self.onExec_Prep_impl()
		self.onExec_Main_impl()
		return None

	def addArg_DataChannel(self, name_chan, wreq_args, tmp_tokchan):
		global gMap_TaskChans
		idx_map_find = self._gmap_TaskChans_index(name_chan, wreq_args)
		if idx_map_find <  0:
			return -1
		wreq_args_map = gMap_TaskChans[idx_map_find]['wreq_args']
		idx_chan_new  = self.__priv_Dwreq2Idx(name_chan, wreq_args_map)
		if idx_chan_new >= 0:
			return idx_chan_new
		# try to add new data channel
		idx_chan_new  = len(self.list_tups_datachan)
		obj_dataset = None
		obj_dataout = None
		obj_dataset = self.onChan_DataSet_alloc(name_chan, wreq_args_map)
		if obj_dataset == None:
			return -1
		obj_dataset.locSet_DbTbl(self._gmap_TaskChans_dbtbl(name_chan, gMap_TaskChans[idx_map_find]['dict_args']))
		obj_dataout = self.onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args_map)
		self.list_tups_datachan.append((obj_dataset, obj_dataout, name_chan, wreq_args_map, gMap_TaskChans[idx_map_find]['dict_args']))
		return idx_chan_new

	def addObj_DataSource(self, obj_datasrc, **kwargs):
		self.list_tups_datasrc.append((obj_datasrc, dict(kwargs)))

	def datCHK_IsFinish(self):
		return self.onDatCHK_IsFinish_impl()

	def datIN_ChanAdd(self, id_chan, name_chan, wreq_args):
		idx_chan = self.onDatIN_ChanAdd_impl(id_chan, name_chan, wreq_args)
		if idx_chan >= 0:
			self.onDatIN_ChanAdd_ext(idx_chan, id_chan)
		return idx_chan

	def datIN_ChanDel(self, id_chan, name_chan, wreq_args):
		idx_chan = self.onDatIN_ChanDel_impl(id_chan, name_chan, wreq_args)
		if idx_chan >= 0:
			self.onDatIN_ChanDel_ext(idx_chan, id_chan)
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

	def onExec_Init_impl(self, **kwargs):
		return None

	def onExec_Prep_impl(self):
		pass

	def onExec_Main_impl(self):
		while len(self.list_tups_datasrc) >  0:
			tup_datasrc = self.list_tups_datasrc.pop(0)
			tup_datasrc[0].prepRead(**tup_datasrc[1])
			tup_datasrc[0].execReadLoop()
			tup_datasrc[0].closeRead()
			del tup_datasrc

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

	def onChan_DataSet_alloc(self, name_chan, wreq_args):
		obj_dataset = None
		if   name_chan == 'ticker':
			obj_dataset = CTDataSet_Ticker(self.logger, self, wreq_args)
		elif name_chan == 'trades':
			obj_dataset = CTDataSet_ATrades(512, self.logger, self, wreq_args)
		elif name_chan == 'book':
			obj_dataset = CTDataSet_ABooks(self.logger, self, wreq_args)
		elif name_chan == 'candles':
			obj_dataset = CTDataSet_ACandles(512, self.logger, self, wreq_args)
		else:
			obj_dataset = None
		return obj_dataset

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args):
		return None

	def onDatIN_ChanAdd_impl(self, id_chan, name_chan, wreq_args):
		global gMap_TaskChans
		#print("CTDataContainer::onDatIN_ChanAdd_impl() ", id_chan, name_chan, wreq_args)
		idx_map_find = self._gmap_TaskChans_index(name_chan, wreq_args)
		if idx_map_find <  0:
			return -1
		wreq_args_map = gMap_TaskChans[idx_map_find]['wreq_args']
		idx_chan_add = self.__priv_Dwreq2Idx(name_chan, wreq_args_map)
		if idx_chan_add <  0:
			self.logger.error(self.inf_this + " (sbsc): can't handle subscribe, chanId=" +
								str(id_chan) + ", args=" + str(wreq_args))
			return -1
		if self.list_tups_datachan[idx_chan_add][0].id_chan != None:
			self.logger.error(self.inf_this + " (sbsc): can't handle subscribe, chanId=" +
								str(id_chan) + " already subscribed!")
			return -1
		self.list_tups_datachan[idx_chan_add][0].locSet_ChanId(id_chan)
		"""
		self.objs_chan_data[idx_handler].locSet_ChanId(id_chan)
		if self.toks_chan_data[idx_handler].value <  self.tok_this:
			with self.toks_chan_data[idx_handler].get_lock():
				self.toks_chan_data[idx_handler].value = self.tok_this
		self.flag_chan_actv[idx_handler] =  True
		self.num_chan_subscribed += 1
		flag_subscribed   = True
		if self.flag_log_intv:
			self.logger.info(self.inf_this + " (sbsc): chan(idx=" +
						str(idx_handler) + ") subscribed, chanId=" + str(id_chan))
		"""
		return idx_chan_add

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		pass

	def onDatIN_ChanDel_impl(self, id_chan, name_chan, wreq_args):
		global gMap_TaskChans
		#print("CTDataContainer::onDatIN_ChanDel_impl() ", id_chan, name_chan, wreq_args)
		"""
		flag_subscribed   = False
		flag_unsubscribed = False

		for idx_chan, obj_chan in enumerate(self.objs_chan_data):
			if obj_chan.id_chan != id_chan:
				continue
			idx_handler = idx_chan
		if idx_handler <  0:
			self.logger.error(self.inf_this + " (sbsc): can't handle unsubscribe, chanId=" + str(id_chan) +
							", obj=" + str(obj_msg))
		else:
			self.objs_chan_data[idx_handler].locSet_ChanId(-1)
			self.flag_chan_actv[idx_handler] = False
			self.num_chan_unsubscribed += 1
			flag_unsubscribed = True
			if self.flag_log_intv:
				self.logger.info(self.inf_this + " (sbsc): chan(idx=" + str(idx_handler) +
						") unsubscribed, chanId=" + str(id_chan))

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

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

	def onDatIN_DataFwd_impl(self, id_chan, fmt_data, obj_msg):
		#print("CTDataContainer::onDatIN_DataFwd_impl() ", id_chan, fmt_data, obj_msg)
		obj_dataset = None
		for idx_chan in range(len(self.list_tups_datachan)):
			tup_chan = self.list_tups_datachan[idx_chan]
			if tup_chan[0].id_chan == id_chan:
				obj_dataset = tup_chan[0]
				break
		if obj_dataset == None:
			self.logger.error(self.inf_this + " (data): can't handle data, chanId:" + str(id_chan) + ", data:" + str(obj_msg))
		else:
			obj_dataset.locDataAppend(fmt_data, obj_msg)

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		pass

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		pass

	def __priv_Dwreq2Idx(self, name_chan, wreq_args_map):
		idx_chan_find = -1
		for idx_chan in range(len(self.list_tups_datachan)):
			tup_chan = self.list_tups_datachan[idx_chan]
			if tup_chan[2] != name_chan:
				continue
			if tup_chan[3] != wreq_args_map:
				continue
			idx_chan_find = idx_chan
			break
		return idx_chan_find

	def __priv_Dset2Idx(self, obj_dataset):
		idx_chan_find = -1
		for idx_chan in range(len(self.list_tups_datachan)):
			if obj_dataset == self.list_tups_datachan[idx_chan][0]:
				idx_chan_find = idx_chan
				break
		return idx_chan_find

	@staticmethod
	def _gmap_TaskChans_init():
		global gMap_TaskChans
		for idx_map in range(len(gMap_TaskChans)):
			try:
				dict_args = json.loads(gMap_TaskChans[idx_map]['wreq_args'])
			except:
				dict_args = None
			gMap_TaskChans[idx_map]['dict_args'] = dict_args
		return True

	@staticmethod
	def _gmap_TaskChans_index(name_chan, wreq_args):
		global gMap_TaskChans
		idx_map_find = -1
		# compose dict_args from wreq_args
		if isinstance(wreq_args, dict):
			dict_args = wreq_args
		else:
			try:
				dict_args = json.loads(wreq_args)
			except:
				dict_args = None
		if dict_args == None:
			return idx_map_find
		# evaluate idx_map_find
		for idx_map in range(len(gMap_TaskChans)):
			if gMap_TaskChans[idx_map]['channel'] != name_chan:
				continue
			idx_map_find = idx_map
			for key, val in gMap_TaskChans[idx_map]['dict_args'].items():
				if str(val) != str(dict_args[key]):
					idx_map_find = -1
					break
			if idx_map_find >= 0:
				break
		return idx_map_find

	@staticmethod
	def _gmap_TaskChans_dbtbl(name_chan, wreq_args):
		global gMap_TaskChans
		idx_map_find = CTDataContainer._gmap_TaskChans_index(name_chan, wreq_args)
		name_dbtbl   = None if idx_map_find <  0 else gMap_TaskChans[idx_map_find]['name_dbtbl']
		return name_dbtbl


