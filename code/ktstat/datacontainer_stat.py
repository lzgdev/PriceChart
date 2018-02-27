
import sys
import json

import math
import copy

import ktdata
import kthttp

dsrc_down_trades = {
			#'switch': False,
			'url': 'mongodb://127.0.0.1:27017/bfx-down',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'limit': 256, 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_down_candles = {
			#'switch': False,
			'url': 'mongodb://127.0.0.1:27017/bfx-down',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', 'load_args': { 'limit': 256, 'sort': [('$natural', 1)], }, },
			],
		}


class CTDataContainer_StatOut(kthttp.CTDataContainer_HttpOut):
	def __init__(self, logger, obj_outconn):
		kthttp.CTDataContainer_HttpOut.__init__(self, logger, obj_outconn)
		self.flag_stat = False
		self.flag_dbg  = False

		self.flag_stat_end  = False
		self.run_loop_left  = 200

		self.stat_mts_step  = 60 * 1000
		self.stat_mts_bgn   = 1510685520000
		#self.stat_mts_bgn   = 1500685520000
		self.stat_mts_bgn   = round(1518961129634 / self.stat_mts_step) * self.stat_mts_step

		self.stat_mts_now   = self.stat_mts_bgn
		self.stat_rec_now   = CTStatRec_Stat11(self.logger, self.stat_mts_step)
		self.stat_rec_now.reset(self.stat_mts_now)

		self.read_trades_tid_last   = None
		self.read_candles_mts_last  = None
		self.read_trades_num    = None
		self.read_candles_num   = None

		self.run_loop_left  = 20
		#self.size_dset_candles = 2

	def statInit(self):
		return self.onStat_InitEntr_impl()

	def getStat_ExecCfg(self):
		return self.onStat_ExecCfg_init()

	def onStat_InitEntr_impl(self):
		return None

	def onStat_ExecCfg_init(self):
		#print("CTDataContainer_Down1Out::onStat_ExecCfg_init(00)")
		list_dsrc_stat = []
		if self.read_trades_num  != 0:
			dsrc_cfg = copy.copy(dsrc_down_trades)
			dsrc_cfg['chans'][0]['load_args']['filter'] = { 'mts': { '$gte': self.stat_mts_now, } } \
							if self.read_trades_tid_last == None else \
								{ 'tid': { '$gt': self.read_trades_tid_last, } }
			dsrc_cfg['chans'][0]['load_args']['limit']  = self.read_trades_num \
							if self.read_trades_num  != None else self.size_dset_trades
			list_dsrc_stat.append(dsrc_cfg)
		if self.read_candles_num != 0:
			dsrc_cfg = copy.copy(dsrc_down_candles)
			dsrc_cfg['chans'][0]['load_args']['filter'] = { 'mts': { '$gte': self.stat_mts_now, } }  \
							if self.read_candles_mts_last == None else \
								{ 'mts': { '$gt': self.read_candles_mts_last, } }
			dsrc_cfg['chans'][0]['load_args']['limit']  = self.read_candles_num \
							if self.read_candles_num != None else self.size_dset_candles
			list_dsrc_stat.append(dsrc_cfg)
		return list_dsrc_stat

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_StatOut::onExec_Prep_impl(01)", self.obj_dset_trades, self.obj_dset_candles)

		#self.read_trades_num   = 0
		#self.read_candles_num  = 0

		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.run_loop_left   -= 1
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_StatOut::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))

		self.obj_dset_trades  = None
		self.obj_dset_candles = None
		num_chans = len(self.list_tups_datachan)
		for idx_chan in range(num_chans):
			if   isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ATrades):
				self.obj_dset_trades  = self.list_tups_datachan[idx_chan][0]
			elif isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ACandles):
				self.obj_dset_candles = self.list_tups_datachan[idx_chan][0]
		#print("CTDataContainer_StatOut::onExec_Prep_impl(11)", self.obj_dset_trades, self.obj_dset_candles)
		if self.flag_stat and self.obj_dset_trades != None and self.obj_dset_candles != None:
			#self.onStat_Loop_impl()
			while True:
				ret_stat = self.onStat_Step_impl()
				if not ret_stat:
					break

		# update self.read_trades_tid_last and self.read_trades_num
		self.read_trades_num   = 0
		len_list  = len(self.obj_dset_trades.loc_trades_recs)
		if len_list <= self.size_dset_trades/2:
			if len_list >  0:
				self.read_trades_tid_last  = self.obj_dset_trades.loc_trades_recs[len_list-1].get('tid', None)
			self.read_trades_num   = self.size_dset_trades  - len_list
		# update self.read_candles_mts_last and self.read_candles_num 
		self.read_candles_num  = 0
		len_list  = len(self.obj_dset_candles.loc_candle_recs)
		if len_list <= self.size_dset_candles/2:
			if len_list >  0:
				self.read_candles_mts_last = self.obj_dset_candles.loc_candle_recs[len_list-1].get('mts', None)
			self.read_candles_num  = self.size_dset_candles - len_list

		self.flag_stat_end  = True if self.run_loop_left <= 0 else False

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
			self.read_trades_tid_last  = rec_trades.get('tid', None)

		rec_candles = None
		while len(self.obj_dset_candles.loc_candle_recs) >  0:
			if self.obj_dset_candles.loc_candle_recs[0]['mts'] >= mts_next:
				flag_next_candles = True
				break
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(0)
			self.stat_rec_now.addRec_Candles(rec_candles)
			self.read_candles_mts_last = rec_candles.get('mts', None)


		if flag_next_trade and flag_next_candles:
			self.stat_mts_now = mts_next
			self.stat_rec_now.finish()
			self.stat_rec_now.dbgDump()
			self.stat_rec_now.reset(self.stat_mts_now)
		return flag_next_trade and flag_next_candles


class CTStatRec(object):
	flag_dbg_stat  = 0

	def __init__(self, logger):
		self.logger = logger

		#self.flag_dbg_stat = 2

	def reset(self, mts):
		return self.onReset_impl(mts)

	def finish(self):
		return self.onFinish_impl()

	def dbgDump(self):
		return self.onDbg_Dump_impl()

	def addRec_Trades(self, rec_trades):
		return self.onAdd_RecTrades(rec_trades)

	def addRec_Candles(self, rec_candles):
		return self.onAdd_RecCandles(rec_candles)

	def onReset_impl(self, mts):
		pass

	def onFinish_impl(self):
		pass

	def onAdd_RecTrades(self, rec_trades):
		return 0

	def onAdd_RecCandles(self, rec_candles):
		return 0

	def onDbg_Dump_impl(self):
		return 0


class CTStatRec_Stat11(CTStatRec):
	def __init__(self, logger, mts_step):
		super(CTStatRec_Stat11, self).__init__(logger)
		self.rec_mts_step = mts_step
		self.rec_mts_this = None

	def onReset_impl(self, mts):
		self.rec_mts_this = mts
		self.rec_mts_next = self.rec_mts_step + self.rec_mts_this
		self.ref_candles  = None
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

	def onFinish_impl(self):
		# NEED to check self.ref_candles['volume'] - self.vol_all
		pass

	def onAdd_RecTrades(self, rec_trades):
		mts_rec = rec_trades.get('mts', -1)
		if self.flag_dbg_stat >  0:
			mts_rec = rec_trades['mts']
			print("CTStatRec_Stat11::onAdd_RecTrades, mts:", format(mts_rec, ","), ", trade:", rec_trades)
		if mts_rec <  self.rec_mts_this or mts_rec >= self.rec_mts_next:
			return 0
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
		return 1

	def onAdd_RecCandles(self, rec_candles):
		mts_rec = rec_candles.get('mts', -1)
		if self.flag_dbg_stat >  0:
			print("CTStatRec_Stat11::onAdd_RecCandles, mts:", format(mts_rec, ","), ", candle:", rec_candles)
		if mts_rec <  self.rec_mts_this or mts_rec >= self.rec_mts_next:
			return 0
		self.ref_candles  = rec_candles
		self.price_high   = rec_candles['high']
		self.price_low    = rec_candles['low']
		self.price_open   = rec_candles['open']
		self.price_close  = rec_candles['close']
		return 1

	def onDbg_Dump_impl(self):
		vol_diff = self.vol_all - (0.0 if self.ref_candles == None else self.ref_candles.get('volume', 0.0))
		print("CTStatRec_Stat11::dump(00), mts:", format(self.rec_mts_this, ","), ", num:", self.num_trades,
								", min:", self.price_low, ", max:", self.price_high,
								", vol:", self.vol_all, ", sum:", self.sum_all,
								", vol_diff:", vol_diff)


