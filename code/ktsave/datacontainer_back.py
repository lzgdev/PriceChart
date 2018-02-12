
import json
import copy
import math

import urllib.parse

import ktdata

from .dataset_back      import CTDataSet_Ticker_back, CTDataSet_ATrades_back, CTDataSet_ABooks_back, CTDataSet_ACandles_back

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


dsrc_db_trades = {
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_db_candles = {
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', 'load_args': { 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_http_trades = {
			'url': 'https://api.bitfinex.com/v2',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', },
			],
		}

dsrc_http_candles = {
			'url': 'https://api.bitfinex.com/v2',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', },
			],
		}


class CTDataContainer_BackOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.flag_back_end  = False
		self.back_mts_step  = 60000
		self.back_mts_bgn   = 1364774820000

		self.back_mts_now   = None
		self.back_rec_now   = CTBackRec_Check(self.logger, self.back_mts_step)

		self.flag_http_mode = False

		self.obj_dset_trades    = None
		self.obj_dset_candles   = None

		self.run_loop_main  = 200

		#self.back_mts_bgn   = 1510685520000
		#self.back_mts_bgn   = 1510686960000
		#self.back_mts_bgn   = 1510709100000

		#self.run_loop_main  =   2
		#self.size_dset_trades  = 8
		#self.size_dset_candles = 8

	def backInit(self):
		return self.onBack_InitEntr_impl()

	def getBack_ExecCfg(self):
		return self.onBack_ExecCfg_init()

	def onInit_DataSource_alloc(self, url_scheme, url_netloc, url):
		obj_datasrc = None
		if   url_scheme == 'https' and url_netloc == 'api.bitfinex.com':
			obj_datasrc = CTDataInput_HttpBfx_Back(self.logger, self, url,
								self.back_mts_now, self.back_mts_now+self.back_mts_step)
		if obj_datasrc == None:
			obj_datasrc = super(CTDataContainer_BackOut, self).onInit_DataSource_alloc(url_scheme, url_netloc, url)
		return obj_datasrc

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
		obj_dataset = None
		if   name_chan == 'ticker':
			obj_dataset = CTDataSet_Ticker_back(self.logger, self, wreq_args)
		elif name_chan == 'trades':
			obj_dataset = CTDataSet_ATrades_back(self.logger, self.size_dset_trades, self, wreq_args)
			self.obj_dset_trades   = obj_dataset
		elif name_chan == 'book':
			obj_dataset = CTDataSet_ABooks_back(self.logger, self, wreq_args)
		elif name_chan == 'candles':
			obj_dataset = CTDataSet_ACandles_back(self.logger, self.size_dset_candles, self, wreq_args)
			self.obj_dset_candles  = obj_dataset
		else:
			obj_dataset = None
		if obj_dataset == None:
			obj_dataset = super(CTDataContainer_BackOut, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		return obj_dataset

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_BackOut::onExec_Prep_impl(01)")
		self.run_loop_main    -= 1
		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_BackOut::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		if self.obj_dset_trades != None and self.obj_dset_candles != None:
			self.onBack_Loop_impl()

		self.flag_back_end  = True if self.run_loop_main <  0 else False

	def onBack_InitEntr_impl(self):
		mts_last = self.back_rec_now.dbInit(self)
		if mts_last == None or mts_last <  self.back_mts_bgn:
			self.back_mts_now = self.back_mts_bgn
		else:
			self.back_mts_now = mts_last + self.back_mts_step
		self.back_rec_now.recReset(self.back_mts_now)
		return None

	def onBack_ExecCfg_init(self):
		#print("CTDataContainer_BackOut::onBack_ExecCfg_init(00)")
		list_tasks_run = []
		if self.flag_http_mode:
			dsrc_cfg = copy.copy(dsrc_http_candles)
			list_tasks_run.append(dsrc_cfg)
			dsrc_cfg = copy.copy(dsrc_http_trades)
			list_tasks_run.append(dsrc_cfg)
			return list_tasks_run

		# evaluate num_rec_read for candles
		num_rec_read = 0
		len_list  = 0 if self.obj_dset_candles == None else len(self.obj_dset_candles.loc_candle_recs)
		if len_list <  self.size_dset_candles/2:
			num_rec_read = self.size_dset_candles - len_list
		#
		if num_rec_read > 0:
			dsrc_cfg = copy.copy(dsrc_db_candles)
			dsrc_db_load = dsrc_cfg['chans'][0]['load_args']
			mts_last = None
			if self.obj_dset_candles != None and self.obj_dset_candles.back_rec_last != None:
				mts_last = self.obj_dset_candles.back_rec_last.get('mts', None)
			if mts_last == None:
				dsrc_db_load['filter'] = { 'mts': { '$gte': self.back_mts_now, } }
			else:
				dsrc_db_load['filter'] = { 'mts': { '$gt': mts_last, } }
			dsrc_db_load['limit']  = num_rec_read
			list_tasks_run.append(dsrc_cfg)
		# evaluate num_rec_read for trades
		num_rec_read = 0
		len_list  = 0 if self.obj_dset_trades == None else len(self.obj_dset_trades.loc_trades_recs)
		if len_list <  self.size_dset_trades/2:
			num_rec_read = self.size_dset_trades  - len_list
		#
		if num_rec_read >  0:
			dsrc_cfg = copy.copy(dsrc_db_trades)
			dsrc_db_load = dsrc_cfg['chans'][0]['load_args']
			tid_last = None
			if self.obj_dset_trades != None and self.obj_dset_trades.back_rec_last != None:
				tid_last = self.obj_dset_trades.back_rec_last.get('tid', None)
			if tid_last == None:
				dsrc_db_load['filter'] = { 'mts': { '$gte': self.back_mts_now, } }
			else:
				dsrc_db_load['filter'] = { 'tid': { '$gt': tid_last, } }
			dsrc_db_load['limit']  =  num_rec_read
			list_tasks_run.append(dsrc_cfg)

		return list_tasks_run

	def onBack_Loop_impl(self):
		#print("CTDataContainer_BackOut::onBack_Loop_impl ...")
		while True:
			ret_next = self.onBack_Step_impl()
			if not ret_next:
				break
			self.back_mts_now += self.back_mts_step
			self.back_rec_now.recReset(self.back_mts_now)

	def onBack_Step_impl(self):
		#print("CTDataContainer_BackOut::onBack_Step_impl ...")
		flag_next_trade = flag_next_candles = False
		mts_next = self.back_mts_now + self.back_mts_step

		while len(self.obj_dset_trades.loc_trades_recs) >  0:
			if self.obj_dset_trades.loc_trades_recs[0]['mts'] >= mts_next:
				flag_next_trade  = True
				break
			rec_trades = self.obj_dset_trades.loc_trades_recs.pop(0)
			self.back_rec_now.addRec_Trades(ktdata.DFMT_KKAIPRIV, rec_trades)

		while len(self.obj_dset_candles.loc_candle_recs) >  0:
			if self.obj_dset_candles.loc_candle_recs[0]['mts'] >= mts_next:
				flag_next_candles = True
				break
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(0)
			self.back_rec_now.addRec_Candles(ktdata.DFMT_KKAIPRIV, rec_candles)

		flag_next = False
		if flag_next_trade and flag_next_candles:
			flag_next = self.back_rec_now.recCheck(self.flag_http_mode)
			self.back_rec_now.dbgDump(1)
			if flag_next:
				self.back_rec_now.dbRecBack()
			if   flag_next and self.flag_http_mode:
				self.obj_dset_trades.locDataClean()
				self.obj_dset_candles.locDataClean()
			elif not flag_next:
				self.back_rec_now.recReset(self.back_mts_now)
				self.obj_dset_trades.locDataClean()
				self.obj_dset_candles.locDataClean()
			self.flag_http_mode = not flag_next
		return flag_next


class CTDataInput_HttpBfx_Back(ktdata.CTDataInput_HttpBfx):
	def __init__(self, logger, obj_container, url_http_pref, mts_begin, mts_end):
		ktdata.CTDataInput_HttpBfx.__init__(self, logger, obj_container, url_http_pref)
		self.loc_mark_end   = -1
		self.back_mts_begin = mts_begin
		self.back_mts_end   = mts_end

		#self.flag_log_intv  = 1

	def onMark_ChanEnd_impl(self):
		self.loc_mark_end   = self.tup_run_stat[1]

	def onMts_ReqStart_impl(self):
		if self.loc_mark_end == self.tup_run_stat[1]:
			return -1
		obj_dataset = self.obj_container.list_tups_datachan[self.tup_run_stat[2]][0]
		mts_last = None if obj_dataset.back_rec_last == None else obj_dataset.back_rec_last.get('mts', None)
		#print("CTDataInput_HttpBfx_Back::onMts_ReqStart_impl, mts_last:", mts_last, obj_dataset)
		if mts_last == None:
			return self.back_mts_begin
		if mts_last >= self.back_mts_end:
			return -1
		return mts_last


class CTBackRec_Check(object):
	def __init__(self, logger, mts_step=None):
		self.logger = logger
		self.inf_this = "CTBackRec_Check"
		self.rec_mts_step  = 60000 if mts_step == None else round(mts_step)
		self.flag_dbg_back =  0
		# members for database
		self.obj_dbwriter  = None
		self.name_dbtbl_trades  = None
		self.name_dbtbl_candles = None
		self.rec_last_trades  = None
		self.rec_last_candles = None
		# record data/ref members of a period time
		self.mts_rec_this  = None
		self.mts_rec_next  = None

		self.list_trades   = []
		self.rec_candles   = None

		self.loc_vol_trades = None
		self.loc_vol_diff   = None

		#self.flag_dbg_back =  2

	def dbInit(self, obj_container):
		return self.onDb_Init_impl(obj_container)

	def recReset(self, mts):
		self.onRec_Reset_impl(mts)

	def recCheck(self, bHttpMode):
		return self.onRec_Check_impl(bHttpMode)

	def dbRecBack(self):
		return self.onDb_RecBack_impl()

	def dbgDump(self, dbgLevel):
		return self.onDbg_Dump_impl(dbgLevel)

	def addRec_Trades(self, fmt_data, rec_trades):
		return self.onAdd_RecTrades(fmt_data, rec_trades)

	def addRec_Candles(self, fmt_data, rec_candles):
		return self.onAdd_RecCandles(fmt_data, rec_candles)

	def onDb_Init_impl(self, obj_container):
		# open database
		str_db_uri  = 'mongodb://127.0.0.1:27017'
		str_db_name = 'bfx-bck01'
		self.obj_dbwriter = ktdata.KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
		# create/open db table for trades
		cfg_chan = dsrc_db_trades['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_trades  = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_trades, map_chan['channel'], map_chan['wreq_args'])
		# create/open db table for candles
		cfg_chan = dsrc_db_candles['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_candles = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_candles, map_chan['channel'], map_chan['wreq_args'])
		# load last back record from database
		self.rec_last_trades  = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_trades, { }, [('$natural', -1)])
		self.rec_last_candles = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_candles, { }, [('$natural', -1)])
		mts_last = None if self.rec_last_candles == None else self.rec_last_candles.get('mts', None)
		print("onDb_Init_impl, mts_last:", mts_last, ", rec_last:", self.rec_last_candles)
		return mts_last

	def onRec_Reset_impl(self, mts):
		mts_this = math.floor(mts / self.rec_mts_step) * self.rec_mts_step
		flag_dup = True if self.mts_rec_this == mts_this else False
		self.mts_rec_this  = mts_this
		self.mts_rec_next  = self.mts_rec_this + self.rec_mts_step
		self.inf_this = "CTBackRec_Check(mts=" + format(self.mts_rec_this, ",") + ")"
		# reset local data members
		self.list_trades.clear()
		self.rec_candles    = None
		self.loc_vol_trades = 0.0
		if not flag_dup:
			self.loc_vol_diff = None

	def onRec_Check_impl(self, bHttpMode):
		vol_candles = 0.0 if self.rec_candles == None else self.rec_candles.get('volume', 0.0)
		vol_diff = abs(vol_candles - self.loc_vol_trades)
		if vol_diff <  0.001:
			return  True
		#print("onRec_Check_impl, mts:", self.mts_rec_this, ", mode:", bHttpMode, ", vol_diff:", vol_diff, self.loc_vol_diff)
		if bHttpMode and vol_diff == self.loc_vol_diff:
			return  True
		self.loc_vol_diff  = vol_diff
		return False

	def onAdd_RecTrades(self, fmt_data, rec_trades):
		if self.flag_dbg_back >= 3:
			self.logger.debug(self.inf_this + ": add trade=" + str(rec_trades))
		mts_rec = rec_trades.get('mts', None)
		if   mts_rec == None:
			self.logger.error(self.inf_this + " add trade ERROR: no mts for trade=" + str(rec_trades))
			return False
		elif mts_rec <  self.mts_rec_this:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts <  this, trade=" + str(rec_trades))
			return False
		elif mts_rec >= self.mts_rec_next:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts >= this, trade=" + str(rec_trades))
			return False
		tid_new = rec_trades.get('tid', None)
		mts_new = rec_trades.get('mts', None)
		if tid_new == None or mts_new == None:
			return False
		len_rec = len(self.list_trades)
		idx_rec = len_rec - 1
		while idx_rec >= 0:
			if tid_new >= self.list_trades[idx_rec]['tid']:
				break
			if mts_new >  self.list_trades[idx_rec]['mts']:
				break
			idx_rec -= 1
		if idx_rec >= 0 and tid_new == self.list_trades[idx_rec]['tid']:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore dup record tid, trade=" + str(rec_trades))
			return False
		self.list_trades.insert(idx_rec+1, rec_trades)
		self.loc_vol_trades += abs(rec_trades.get('amount', 0.0))
		return True

	def onAdd_RecCandles(self, fmt_data, rec_candles):
		if self.flag_dbg_back >= 2:
			self.logger.debug(self.inf_this + ": add candles=" + str(rec_candles))
		mts_rec = rec_candles.get('mts', None)
		if mts_rec != self.mts_rec_this:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts != this, candles=" + str(rec_candles))
			return False
		self.rec_candles   = rec_candles
		return True

	def onDb_RecBack_impl(self):
		for rec_trades in self.list_trades:
			tid_last = -1 if self.rec_last_trades  == None else self.rec_last_trades.get('tid', -1)
			tid_this = rec_trades.get('tid', -1)
			if tid_this >  tid_last:
				self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_trades, rec_trades)
				self.rec_last_trades  = rec_trades
		if self.rec_candles != None:
			mts_last = -1 if self.rec_last_candles == None else self.rec_last_candles.get('mts', -1)
			mts_this = self.rec_candles.get('mts', -1)
			if mts_this >  mts_last:
				self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_candles, self.rec_candles)
				self.rec_last_candles = self.rec_candles
		return None

	def onDbg_Dump_impl(self, dbgLevel):
		vol_candles = self.rec_candles.get('volume', 0.0) if self.rec_candles != None else 0.0
		diff_vol = abs(vol_candles - self.loc_vol_trades)
		if dbgLevel >= 1:
			print(self.inf_this, "dump(00), num:", len(self.list_trades), ", diff_vol:", round(diff_vol, 3), "|", vol_candles, self.loc_vol_trades)
		if dbgLevel >= 2  or self.rec_candles == None:
			print(self.inf_this, "dump(10), rec_candles:", self.rec_candles)
		if dbgLevel >= 3  or abs(diff_vol) >= 0.001:
			for idx_trades in range(len(self.list_trades)):
				rec_trades = self.list_trades[idx_trades]
				mts_trades = rec_trades['mts']
				print(self.inf_this, "dump(12), idx:", str(idx_trades).zfill(3), ", trades:",  rec_trades)



