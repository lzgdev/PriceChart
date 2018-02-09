
import json
import math

import urllib.parse

import ktdata

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


class CTDataContainer_BackOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.back_mts_step = 60000

		self.obj_dbwriter   = None

		self.flag_back_end  = False

		self.back_mts_step  = 60 * 1000
		self.back_mts_bgn   = 1510685520000

		self.back_mts_now   = self.back_mts_bgn
		self.back_rec_now   = CTBackRec_Check(self.logger, self.back_mts_step)
		self.back_rec_now.reset(self.back_mts_now)

		self.read_trades_tid_last   = None
		self.read_candles_mts_last  = None
		self.read_trades_num    = 0
		self.read_candles_num   = 0

		self.run_loop_main  = 200

		#self.run_loop_main  =   2

		# debug settings
		#self.read_trades_num    = 20
		#self.read_candles_num   = 20

#	def onExec_Init_impl(self, list_task):
#		if len(list_task) == 0:
#			return None
#		args_task = list_task.pop()
#
#		task_url  = args_task.get('url', None)
#		task_jobs = args_task.get('jobs', None)
#		msec_off  = args_task.get('msec_off', None)
#
#		str_db_uri  = 'mongodb://127.0.0.1:27017'
#		str_db_name = 'bfx-pub'
#		self.obj_dbwriter  = ktdata.KTDataMedia_DbWriter(self.logger)
#		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
#
#		url_parse = urllib.parse.urlparse(task_url)
#		if   url_parse.scheme == 'https' and url_parse.netloc == 'api.bitfinex.com':
#			self.addObj_DataSource(ktdata.CTDataInput_HttpBfx(self.logger, self, task_url))
#		elif url_parse.scheme ==   'wss' and url_parse.netloc == 'api.bitfinex.com':
#			self.addObj_DataSource(ktdata.CTDataInput_WssBfx(self.logger, self, task_url,
#								self.tok_mono_this, msec_off))
#
#		for map_unit in task_jobs:
#			self.addArg_DataChannel(map_unit['channel'], map_unit['wreq_args'])
#
#		return None
#
#	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
#		obj_dataout = None
#		if   name_chan == 'ticker':
#			obj_dataout = CTDataOut_Db_ticker(self.logger, obj_dataset, self.obj_dbwriter)
#		elif name_chan == 'trades':
#			obj_dataout = CTDataOut_Db_trades(self.logger, obj_dataset, self.obj_dbwriter)
#		elif name_chan == 'book':
#			obj_dataout = CTDataOut_Db_book(self.logger, obj_dataset, self.obj_dbwriter)
#		elif name_chan == 'candles':
#			obj_dataout = CTDataOut_Db_candles(self.logger, obj_dataset, self.obj_dbwriter)
#		return obj_dataout
#
#	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
#		tup_chan  = self.list_tups_datachan[idx_chan]
#		obj_dataset = tup_chan[0]
#		obj_dataout = tup_chan[1]
#		#print("CTDataContainer_BackOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
#		if obj_dataout != None:
#			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
#								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_BackOut::onExec_Prep_impl(01)", self.obj_dset_trades, self.obj_dset_candles)
		self.run_loop_main    -= 1

		self.read_trades_num   = 0
		self.read_candles_num  = 0

		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_BackOut::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))

		self.obj_dset_trades  = None
		self.obj_dset_candles = None
		num_chans = len(self.list_tups_datachan)
		for idx_chan in range(num_chans):
			if   isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ATrades):
				self.obj_dset_trades  = self.list_tups_datachan[idx_chan][0]
			elif isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ACandles):
				self.obj_dset_candles = self.list_tups_datachan[idx_chan][0]
		#print("CTDataContainer_BackOut::onExec_Post_impl(11)", self.obj_dset_trades, self.obj_dset_candles)

		if self.obj_dset_trades != None and self.obj_dset_candles != None:
			self.onBack_Loop_impl()

		# update self.read_trades_tid_last and self.read_trades_num
		self.read_trades_num   = 0
		len_list  = 0 if self.obj_dset_trades == None else len(self.obj_dset_trades.loc_trades_recs)
		if len_list <  self.size_dset_trades/2:
			if len_list >  0:
				rec_trades = self.obj_dset_trades.loc_trades_recs[len_list-1]
				self.read_trades_tid_last  = rec_trades['tid']
			self.read_trades_num   = self.size_dset_trades  - len_list
		# update self.read_candles_mts_last and self.read_candles_num
		self.read_candles_num  = 0
		len_list  = 0 if self.obj_dset_candles == None else len(self.obj_dset_candles.loc_candle_recs)
		if len_list <  self.size_dset_candles/2:
			if len_list >  0:
				rec_candles = self.obj_dset_candles.loc_candle_recs[len_list-1]
				self.read_candles_mts_last = rec_candles['mts']
			self.read_candles_num  = self.size_dset_candles - len_list

		#print("CTDataContainer_BackOut::onExec_Post_impl(99)",
		#		", trades:", self.read_trades_num, self.read_trades_tid_last,
		#		", candles:", self.read_candles_num, self.read_candles_mts_last)
		self.flag_back_end  = True if self.run_loop_main <  0 else False

	def onBack_Loop_impl(self):
		#print("CTDataContainer_BackOut::onBack_Loop_impl ...")
		while True:
			ret_step = self.onBack_Step_impl()
			if not ret_step:
				break

	def onBack_Step_impl(self):
		#print("CTDataContainer_BackOut::onBack_Step_impl ...")
		flag_next_trade = flag_next_candles = False
		mts_stat = self.back_mts_now
		mts_next = self.back_mts_now + self.back_mts_step

		while len(self.obj_dset_trades.loc_trades_recs) >  0:
			if self.obj_dset_trades.loc_trades_recs[0]['mts'] >= mts_next:
				flag_next_trade  = True
				break
			rec_trades = self.obj_dset_trades.loc_trades_recs.pop(0)
			ret_add = self.back_rec_now.addRec_Trades(ktdata.DFMT_KKAIPRIV, rec_trades)
			if ret_add:
				self.read_trades_tid_last  = rec_trades['tid']

		while len(self.obj_dset_candles.loc_candle_recs) >  0:
			if self.obj_dset_candles.loc_candle_recs[0]['mts'] >= mts_next:
				flag_next_candles = True
				break
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(0)
			ret_add = self.back_rec_now.addRec_Candles(ktdata.DFMT_KKAIPRIV, rec_candles)
			if ret_add:
				self.read_candles_mts_last = rec_candles['mts']

		if flag_next_trade and flag_next_candles:
			self.back_mts_now = mts_next
			self.back_rec_now.check()
			self.back_rec_now.dbgDump(1)
			self.back_rec_now.reset(self.back_mts_now)
		return flag_next_trade and flag_next_candles


class CTBackRec_Check(object):
	def __init__(self, logger, mts_step=None):
		self.logger = logger
		self.inf_this = "CTBackRec_Check"
		self.rec_mts_step  = 60000 if mts_step == None else round(mts_step)
		self.flag_dbg_back =  0
		# record data/ref members of a period time
		self.rec_mts_this  = None
		self.rec_mts_next  = None

		self.list_trades   = []
		self.rec_candles   = None

		self.loc_vol_trades = None

		#self.flag_dbg_back =  2

	def reset(self, mts):
		self.onReset_impl(mts)

	def check(self):
		return self.onCheck_impl()

	def dbgDump(self, dbgLevel):
		return self.onDbg_Dump_impl(dbgLevel)

	def addRec_Trades(self, fmt_data, rec_trades):
		return self.onAdd_RecTrades(fmt_data, rec_trades)

	def addRec_Candles(self, fmt_data, rec_candles):
		return self.onAdd_RecCandles(fmt_data, rec_candles)

	def onReset_impl(self, mts):
		self.rec_mts_this  = math.floor(mts / self.rec_mts_step) * self.rec_mts_step
		self.rec_mts_next  = self.rec_mts_this + self.rec_mts_step
		self.inf_this = "CTBackRec_Check(mts=" + format(self.rec_mts_this, ",") + ")"
		# reset local data members
		self.list_trades.clear()
		self.rec_candles   = None
		self.loc_vol_trades = 0.0

	def onCheck_impl(self):
		if self.rec_candles == None:
			return False
		vol_candles = self.rec_candles['volume']
		vol_trades  = 0.0
		for rec_trades in self.list_trades:
			vol_trades += abs(rec_trades['amount'])
		vol_diff = abs(vol_candles - vol_trades)
		return True if vol_diff <  0.001 else False

	def onAdd_RecTrades(self, fmt_data, rec_trades):
		if self.flag_dbg_back >= 2:
			self.logger.debug(self.inf_this + ": add trade=" + str(rec_trades))
		mts_rec = rec_trades.get('mts', None)
		if   mts_rec == None:
			self.logger.error(self.inf_this + " add trade ERROR: no mts for trade=" + str(rec_trades))
			return False
		elif mts_rec <  self.rec_mts_this:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts <  this, trade=" + str(rec_trades))
			return False
		elif mts_rec >= self.rec_mts_next:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts >= this, trade=" + str(rec_trades))
			return False
		tid_rec = rec_trades.get('tid', None)
		if tid_rec == None:
			return False
		len_rec = len(self.list_trades)
		idx_rec = len_rec - 1
		while idx_rec >= 0:
			if tid_rec >= self.list_trades[idx_rec]['tid']:
				break
			idx_rec -= 1
		if idx_rec >= 0 and tid_rec == self.list_trades[idx_rec]['tid']:
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
		if mts_rec != self.rec_mts_this:
			if self.flag_dbg_back >= 1:
				self.logger.warning(self.inf_this + " ignore record mts != this, candles=" + str(rec_candles))
			return False
		self.rec_candles   = rec_candles
		return True

	def onDbg_Dump_impl(self, dbgLevel):
		diff_vol = (self.rec_candles.get('volume', 0.0) if self.rec_candles != None else 0.0) - self.loc_vol_trades
		if dbgLevel >= 1:
			print(self.inf_this, "dump(00), num:", len(self.list_trades), ", diff_vol:", round(diff_vol, 3))
		if dbgLevel >= 2  or self.rec_candles == None:
			print(self.inf_this, "dump(10), rec_candles:", self.rec_candles)
		if dbgLevel >= 3  or abs(diff_vol) >= 0.001:
			for idx_trades in range(len(self.list_trades)):
				rec_trades = self.list_trades[idx_trades]
				mts_trades = rec_trades['mts']
				print(self.inf_this, "dump(12), idx:", str(idx_trades).zfill(3), ", trades:",  rec_trades)


