
import sys
import json
import math

import ktdata
import kthttp

from .dataset_stat      import CTDataSet_Stat_Dbg, CTDataSet_Stat_Stat01,  \
							CTDataSet_Ticker_Stat, CTDataSet_ATrades_Stat, CTDataSet_ABooks_Stat, CTDataSet_ACandles_Stat

from .dataoutput_stat   import CTDataOut_Stat_stat01,  \
							CTDataOut_Stat_ticker, CTDataOut_Stat_trades, CTDataOut_Stat_book, CTDataOut_Stat_candles


class CTDataContainer_StatOut(kthttp.CTDataContainer_HttpOut):
	num_rec_trades  = 512
	num_rec_candles = 512

	def __init__(self, logger, obj_outconn):
		kthttp.CTDataContainer_HttpOut.__init__(self, logger, obj_outconn)
		self.flag_stat_end  = False

		self.stat_mts_step  = 60 * 1000
		self.stat_mts_bgn   = 1510685520000

		self.stat_mts_now   = self.stat_mts_bgn
		self.stat_rec_now   = CTStatRec_Stat11(self.logger)
		self.stat_rec_now.reset(self.stat_mts_now)

		self.read_trades_tid_last   = None
		self.read_candles_mts_last  = None
		self.read_trades_num    = self.num_rec_trades
		self.read_candles_num   = self.num_rec_candles

		#self.run_loop_left  = 2
		self.run_loop_left  = 200

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
		stat_key  = dict_args['stat'] if 'stat' in dict_args else None
		#print("CTDataContainer_StatOut::onChan_DataSet_alloc", name_chan, wreq_args, stat_key)
		obj_dataset = None
		if   name_chan == 'stat' and stat_key == 'stat01':
			obj_dataset = CTDataSet_Stat_Stat01(512, self.logger, self, wreq_args)
		elif name_chan == 'ticker':
			obj_dataset = CTDataSet_Ticker_Stat(self.logger, self, wreq_args)
		elif name_chan == 'trades':
			obj_dataset = CTDataSet_ATrades_Stat(self.num_rec_trades, self.logger, self, wreq_args)
		elif name_chan == 'book':
			obj_dataset = CTDataSet_ABooks_Stat(self.logger, self, wreq_args)
		elif name_chan == 'candles':
			obj_dataset = CTDataSet_ACandles_Stat(self.num_rec_candles, self.logger, self, wreq_args)
		else:
			obj_dataset = None
		if obj_dataset == None:
			super(CTDataContainer_StatOut, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		return obj_dataset

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		#print("CTDataContainer_StatOut::onChan_DataOut_alloc", name_chan, wreq_args)
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDataOut_Stat_ticker(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'trades':
			obj_dataout = CTDataOut_Stat_trades(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'book':
			obj_dataout = CTDataOut_Stat_book(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'candles':
			obj_dataout = CTDataOut_Stat_candles(self.logger, obj_dataset, self.obj_outconn)
		if obj_dataout == None:
			obj_dataout = super(CTDataContainer_StatOut, self).onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args, dict_args)
		return obj_dataout

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
		if self.obj_dset_trades  != None and self.obj_dset_candles != None:
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


class CTStatRec(object):
	def __init__(self, logger):
		self.logger = logger
		self.flag_dbg_rec =  0

		#self.flag_dbg_rec =  1

	def reset(self, mts):
		self.onReset_impl(mts)

	def finish(self):
		self.onFinish_impl()

	def dbgDump(self):
		self.onDbg_Dump_impl()

	def addRec_Trades(self, rec_trades):
		self.onAdd_RecTrades(rec_trades)

	def addRec_Candles(self, rec_candles):
		self.onAdd_RecCandles(rec_candles)

	def onReset_impl(self, mts):
		pass

	def onFinish_impl(self):
		pass

	def onAdd_RecTrades(self, rec_trades):
		pass

	def onAdd_RecCandles(self, rec_candles):
		pass

	def onDbg_Dump_impl(self):
		pass


class CTStatRec_Stat11(CTStatRec):
	def __init__(self, logger):
		super(CTStatRec_Stat11, self).__init__(logger)

	def onReset_impl(self, mts):
		self.ref_candles  = None
		self.rec_mts  = mts
		self.num_trades   = 0
		self.sum_all      = 0.0
		self.sum_sell     = 0.0
		self.sum_buy      = 0.0
		self.vol_all      = 0.0
		self.vol_sell     = 0.0
		self.vol_buy      = 0.0
		self.price_high   = None
		self.price_low    = None
		self.price_open   = None
		self.price_close  = None

		self.list_trades  = []

	def onFinish_impl(self):
		"""
		diff_vol = self.ref_candles['volume'] - self.vol_all
		print("CTStatRec_Stat11::finish, mts:", format(self.rec_mts, ","), ", diff_vol:", diff_vol,
								", min:", self.price_low, ", max:", self.price_high,
								", vol:", self.vol_all, ", sum:", self.sum_all)
		"""
		pass

	def onAdd_RecTrades(self, rec_trades):
		if self.flag_dbg_rec >  0:
			mts_rec = rec_trades['mts']
			print("CTStatRec_Stat11::onAdd_RecTrades, mts:", format(mts_rec, ","), ", trade:", rec_trades)
		rec_price  = rec_trades['price']
		rec_amount = rec_trades['amount']
		abs_amount = abs(rec_amount)

		self.num_trades  += 1
		sum_add = rec_price * abs_amount
		self.sum_all  += sum_add
		self.vol_all  += abs_amount
		if rec_amount <  0.0:
			self.sum_sell += sum_add
			self.vol_sell += abs_amount
		else:
			self.sum_buy  += sum_add
			self.vol_buy  += abs_amount

		self.list_trades.append(rec_trades)

	def onAdd_RecCandles(self, rec_candles):
		if self.flag_dbg_rec >  0:
			mts_rec = rec_candles['mts']
			print("CTStatRec_Stat11::onAdd_RecCandles, mts:", format(mts_rec, ","), ", candle:", rec_candles)
		self.ref_candles  = rec_candles
		self.price_high   = rec_candles['high']
		self.price_low    = rec_candles['low']
		self.price_open   = rec_candles['open']
		self.price_close  = rec_candles['close']

	def onDbg_Dump_impl(self):
		diff_vol = self.ref_candles['volume'] - self.vol_all
		print("CTStatRec_Stat11::dump(00), mts:", format(self.rec_mts, ","), ", num:", self.num_trades, ", diff_vol:", round(diff_vol, 3),
								", min:", self.price_low, ", max:", self.price_high,
								", vol:", self.vol_all, ", sum:", self.sum_all)
		if abs(diff_vol) >= 0.001:
			print("CTStatRec_Stat11::dump(11), mts:", format(self.rec_mts, ","), ", candles:", self.ref_candles)
			for rec_trades in self.list_trades:
				mts_trades = rec_trades['mts']
				print("                ::dump(12), mts:", format(mts_trades, ","), ", trades:",  rec_trades)



