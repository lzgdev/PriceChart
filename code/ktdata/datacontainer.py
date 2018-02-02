
import json

import os
import time
import multiprocessing

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
		# channel: 18 ~ 21 for statistics
		{ 'channel':    'stat', 'name_dbtbl':    'candles-tBTCUSD-1m', 'wreq_args':  '{ "stat": "stat01" }', },
		{ 'channel':    'stat', 'name_dbtbl':    'candles-tBTCUSD-1m', 'wreq_args':  '{ "stat": "stat02" }', },
		{ 'channel':    'stat', 'name_dbtbl':    'candles-tBTCUSD-1m', 'wreq_args':  '{ "stat": "stat11" }', },
		{ 'channel':    'stat', 'name_dbtbl':    'candles-tBTCUSD-1m', 'wreq_args':  '{ "stat": "stat12" }', },
	]


class CTDataContainer(object):
	def __init__(self, logger):
		global gMap_TaskChans_init
		object.__init__(self)
		self.logger   = logger
		self.tok_mono_this = self.mtsNow_mono()
		self.inf_this = "DataContainer"
		self.pid_this = os.getpid()
		self.list_tups_datasrc  = []
		self.list_tups_datachan = []
		# init global chans map table
		if not gMap_TaskChans_init:
			self._gmap_TaskChans_init()

	def execMain(self, **kwargs):
		ret_init = self.onExec_Init_impl(**kwargs)
		if None != ret_init:
			return ret_init
		self.onExec_Prep_impl()
		self.onExec_Main_impl()
		self.onExec_Post_impl()
		return None

	def addArg_DataChannel(self, name_chan, wreq_args):
		global gMap_TaskChans
		idx_map_find = self._gmap_TaskChans_index(name_chan, wreq_args)
		if idx_map_find <  0:
			return -1
		wreq_args_map = gMap_TaskChans[idx_map_find]['wreq_args']
		dict_args_map = gMap_TaskChans[idx_map_find]['dict_args']
		idx_chan_new  = self.__priv_Dwreq2Idx(name_chan, wreq_args_map)
		if idx_chan_new >= 0:
			return idx_chan_new
		# try to add new data channel
		idx_chan_new  = len(self.list_tups_datachan)
		obj_dataset = None
		obj_dataout = None
		obj_dataset = self.onChan_DataSet_alloc(name_chan, wreq_args_map, dict_args_map)
		if obj_dataset == None:
			return -1
		obj_dataset.locSet_DbTbl(self._gmap_TaskChans_dbtbl(name_chan, dict_args_map))
		obj_dataout = self.onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args_map, dict_args_map)
		self.list_tups_datachan.append((obj_dataset, obj_dataout, name_chan, wreq_args_map, dict_args_map))
		return idx_chan_new

	def addObj_DataSource(self, obj_datasrc, **kwargs):
		self.list_tups_datasrc.append((obj_datasrc, dict(kwargs)))

	def datIN_ChanAdd(self, id_chan, name_chan, wreq_args):
		idx_chan = self.onDatIN_ChanAdd_impl(id_chan, name_chan, wreq_args)
		if idx_chan >= 0:
			self.onDatIN_ChanAdd_ext(idx_chan, id_chan)
		return idx_chan

	def datIN_ChanDel(self, id_chan):
		idx_chan = self.onDatIN_ChanDel_impl(id_chan)
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

	def onExec_Post_impl(self):
		pass

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
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

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
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

	def onDatIN_ChanDel_impl(self, id_chan):
		idx_chan = self.__priv_Dcid2Idx(id_chan)
		#print("CTDataContainer::onDatIN_ChanDel_impl() ", id_chan)
		if idx_chan <  0:
			return idx_chan
		self.list_tups_datachan[idx_chan][0].locSet_ChanId(None)
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
		return idx_chan

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

	def onDatIN_DataFwd_impl(self, id_chan, fmt_data, obj_msg):
		idx_chan = self.__priv_Dcid2Idx(id_chan)
		#print("CTDataContainer::onDatIN_DataFwd_impl() ", id_chan, fmt_data, obj_msg)
		obj_dataset = None if idx_chan <  0 else self.list_tups_datachan[idx_chan][0]
		if obj_dataset == None:
			self.logger.error(self.inf_this + " (data): can't handle data, chanId:" + str(id_chan) + ", data:" + str(obj_msg))
		else:
			obj_dataset.locDataAppend(fmt_data, obj_msg)

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.clrAppend(None)

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.synAppend(msec_now)

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.docAppend(doc_rec)


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
			if obj_dataset != self.list_tups_datachan[idx_chan][0]:
				continue
			idx_chan_find = idx_chan
			break
		return idx_chan_find

	def __priv_Dcid2Idx(self, id_chan):
		idx_chan_find = -1
		if id_chan == None:
			return idx_chan_find
		for idx_chan in range(len(self.list_tups_datachan)):
			if id_chan != self.list_tups_datachan[idx_chan][0].id_chan:
				continue
			idx_chan_find = idx_chan
			break
		return idx_chan_find

	@staticmethod
	def mtsNow_mono():
		time_now = time.clock_gettime(time.CLOCK_MONOTONIC)
		return round(time_now * 1000)

	@staticmethod
	def mtsNow_time():
		time_now = time.time()
		return round(time_now * 1000)

	@staticmethod
	def _gmap_TaskChans_init():
		global gMap_TaskChans, gMap_TaskChans_init
		if gMap_TaskChans_init:
			return gMap_TaskChans_init
		print("INFO: init global data channels map in CTDataContainer ...")
		for idx_map in range(len(gMap_TaskChans)):
			try:
				dict_args = json.loads(gMap_TaskChans[idx_map]['wreq_args'])
			except:
				dict_args = None
			gMap_TaskChans[idx_map]['dict_args'] = dict_args
			gMap_TaskChans[idx_map]['tok_task']  = multiprocessing.Value('l', 0)
		gMap_TaskChans_init = True
		return gMap_TaskChans_init

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
	def _gmap_TaskChans_chan(name_chan, wreq_args):
		global gMap_TaskChans
		idx_map_find = CTDataContainer._gmap_TaskChans_index(name_chan, wreq_args)
		unit_chan    = None if idx_map_find <  0 else gMap_TaskChans[idx_map_find]
		return unit_chan

	@staticmethod
	def _gmap_TaskChans_dbtbl(name_chan, wreq_args):
		global gMap_TaskChans
		idx_map_find = CTDataContainer._gmap_TaskChans_index(name_chan, wreq_args)
		name_dbtbl   = None if idx_map_find <  0 else gMap_TaskChans[idx_map_find]['name_dbtbl']
		return name_dbtbl


