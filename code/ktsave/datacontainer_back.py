
import json
import urllib.parse

import ktdata

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


class CTDataContainer_BackOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.back_mts_step = 60000

		self.obj_dbwriter  = None

		self.back_mts_now  = None

	def onExec_Init_impl(self, list_task):
		if len(list_task) == 0:
			return None
		args_task = list_task.pop()

		task_url  = args_task.get('url', None)
		task_jobs = args_task.get('jobs', None)
		msec_off  = args_task.get('msec_off', None)

		str_db_uri  = 'mongodb://127.0.0.1:27017'
		str_db_name = 'bfx-pub'
		self.obj_dbwriter  = ktdata.KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)

		url_parse = urllib.parse.urlparse(task_url)
		if   url_parse.scheme == 'https' and url_parse.netloc == 'api.bitfinex.com':
			self.addObj_DataSource(ktdata.CTDataInput_HttpBfx(self.logger, self, task_url))
		elif url_parse.scheme ==   'wss' and url_parse.netloc == 'api.bitfinex.com':
			self.addObj_DataSource(ktdata.CTDataInput_WssBfx(self.logger, self, task_url,
								self.tok_mono_this, msec_off))

		for map_unit in task_jobs:
			self.addArg_DataChannel(map_unit['channel'], map_unit['wreq_args'])

		return None

	"""
	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDataOut_Db_ticker(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'trades':
			obj_dataout = CTDataOut_Db_trades(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'book':
			obj_dataout = CTDataOut_Db_book(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'candles':
			obj_dataout = CTDataOut_Db_candles(self.logger, obj_dataset, self.obj_dbwriter)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		#print("CTDataContainer_BackOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		if obj_dataout != None:
			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)
	"""

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_StatOut::onExec_Prep_impl(01)", self.obj_dset_trades, self.obj_dset_candles)
		self.run_loop_left   -= 1
		self.obj_dset_trades  = None
		self.obj_dset_candles = None
		num_chans = len(self.list_tups_datachan)
		for idx_chan in range(num_chans):
			if   isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ATrades):
				self.obj_dset_trades  = self.list_tups_datachan[idx_chan][0]
			elif isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ACandles):
				self.obj_dset_candles = self.list_tups_datachan[idx_chan][0]
		#print("CTDataContainer_StatOut::onExec_Prep_impl(11)", self.obj_dset_trades, self.obj_dset_candles)

		self.read_trades_num   = 0
		self.read_candles_num  = 0

		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_StatOut::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		if self.flag_stat and self.obj_dset_trades != None and self.obj_dset_candles != None:
			self.onStat_Loop_impl()
		self.flag_stat_end  = True if self.run_loop_left <  0 else False

	def onStat_Loop_impl(self):
		while True:
			ret_stat = self.onStat_Step_impl()
			if not ret_stat:
				break

	def onStat_Step_impl(self):
		flag_next_trade = flag_next_candles = False
		mts_stat = self.stat_mts_now
		mts_next = self.stat_mts_now + self.stat_mts_step

		rec_trades  = None
		while len(self.obj_dset_trades.loc_trades_recs) >  0:
			if self.obj_dset_trades.loc_trades_recs[0]['mts'] >= mts_next:
				flag_next_trade  = True
				break
			rec_trades = self.obj_dset_trades.loc_trades_recs.pop(0)
			self.stat_rec_now.addRec_Trades(rec_trades)

		rec_candles = None
		while len(self.obj_dset_candles.loc_candle_recs) >  0:
			if self.obj_dset_candles.loc_candle_recs[0]['mts'] >= mts_next:
				flag_next_candles = True
				break
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(0)
			self.stat_rec_now.addRec_Candles(rec_candles)

		# update self.read_trades_tid_last and self.read_trades_num
		len_list  = len(self.obj_dset_trades.loc_trades_recs)
		if len_list <  self.num_rec_trades/2:
			if len_list >  0:
				rec_trades = self.obj_dset_trades.loc_trades_recs[len_list-1]
			if rec_trades != None:
				self.read_trades_tid_last  = rec_trades['tid']
			self.read_trades_num   = self.num_rec_trades  - len_list
		# update self.read_candles_mts_last and self.read_candles_num 
		len_list  = len(self.obj_dset_candles.loc_candle_recs)
		if len_list <  self.num_rec_candles/2:
			if len_list >  0:
				rec_candles = self.obj_dset_candles.loc_candle_recs[len_list-1]
			if rec_candles != None:
				self.read_candles_mts_last = rec_candles['mts']
			self.read_candles_num  = self.num_rec_candles - len_list

		if flag_next_trade and flag_next_candles:
			self.stat_mts_now = mts_next
			self.stat_rec_now.finish()
			self.stat_rec_now.dbgDump()
			self.stat_rec_now.reset(self.stat_mts_now)
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

		self.flag_dbg_back =  1

	def reset(self, mts):
		self.onReset_impl(mts)

	def check(self):
		return self.onCheck_impl()

	def dbgDump(self):
		self.onDbg_Dump_impl()

	def addRec_Trades(self, fmt_data, rec_trades):
		self.onAdd_RecTrades(fmt_data, rec_trades)

	def addRec_Candles(self, fmt_data, rec_candles):
		self.onAdd_RecCandles(fmt_data, rec_candles)

	def onReset_impl(self, mts):
		self.rec_mts_this  = math.floor(mts / self.rec_mts_step) * self.rec_mts_step
		self.rec_mts_next  = self.rec_mts_this + self.rec_mts_step
		self.inf_this = "CTBackRec_Check(mts=" + format(self.rec_mts_this, ",") + ")"

		self.list_trades.clear()
		self.rec_candles   = None

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

	def onDbg_Dump_impl(self):
		diff_vol = self.rec_candles['volume'] - self.vol_all
		print("CTStatRec_Stat11::dump(00), mts:", format(self.rec_mts_this, ","), ", num:", self.num_trades, ", diff_vol:", round(diff_vol, 3),
								", min:", self.price_low, ", max:", self.price_high,
								", vol:", self.vol_all, ", sum:", self.sum_all)
		if abs(diff_vol) >= 0.001:
			print("CTStatRec_Stat11::dump(11), mts:", format(self.rec_mts_this, ","), ", candles:", self.rec_candles)
			list_vols = []
			idx_trades  = 0
			for rec_trades in self.list_trades:
				mts_trades = rec_trades['mts']
				print("                ::dump(12), mts:", format(mts_trades, ","), ", idx:", idx_trades, ", trades:",  rec_trades)
				list_vols.append(abs(rec_trades['amount']))
				idx_trades += 1
			#print("CTStatRec_Stat11::dump(21), mts:", format(self.rec_mts_this, ","), ", list_orig:", list_vols)
			#list_vols.sort()
			#print("CTStatRec_Stat11::dump(21), mts:", format(self.rec_mts_this, ","), ", list_sort:", list_vols)
			vol_new = 0.0
			list_vols.sort()
			for vol_try in list_vols:
				vol_new += vol_try
			diff_new = self.rec_candles['volume'] - vol_new
			print("CTStatRec_Stat11::dump(01), mts:", format(self.rec_mts_this, ","), ", num:", self.num_trades, ", diff_new:", round(diff_new, 3), ", vol_new:", vol_new)



