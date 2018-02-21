
import json
import copy
import math

import urllib.parse

import ktdata

from .dataset_back      import CTDataSet_Ticker_back, CTDataSet_ATrades_back, CTDataSet_ABooks_back, CTDataSet_ACandles_back

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


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


class CTDataContainer_Down1Out(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.back_mts_step  = 60000
		self.back_mts_bgn   = 1364774820000

		self.down_rec_save  = CTDown1Rec_Save(self.logger, self.back_mts_step)

		self.obj_dset_trades    = None
		self.obj_dset_candles   = None

		self.run_loop_main  = 200

		#self.run_loop_main  =   2
		#self.size_dset_trades  = 8
		#self.size_dset_candles = 8

	def downInit(self):
		return self.onDown_InitEntr_impl()

	def getDown_ExecCfg(self):
		return self.onDown_ExecCfg_init()

	def onInit_DataSource_alloc(self, url_scheme, url_netloc, url):
		obj_datasrc = None
		if url_scheme != 'https' or url_netloc != 'api.bitfinex.com':
			return None
		obj_datasrc = CTDataInput_HttpBfx_Down1(self.logger, self, url)
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
			obj_dataset = super(CTDataContainer_Down1Out, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		return obj_dataset

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_Down1Out::onExec_Prep_impl(01)")
		self.run_loop_main    -= 1
		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_Down1Out::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		#if self.obj_dset_trades != None and self.obj_dset_candles != None:
		#	self.onBack_Loop_impl()

	def onDown_InitEntr_impl(self):
		self.down_rec_save.dbInit(self)
		self.down_rec_save.recReset()
		return None

	def onDown_ExecCfg_init(self):
		#print("CTDataContainer_Down1Out::onDown_ExecCfg_init(00)")
		list_tasks_down = []
		dsrc_cfg = copy.copy(dsrc_http_trades)
		list_tasks_down.append(dsrc_cfg)
		dsrc_cfg = copy.copy(dsrc_http_candles)
		list_tasks_down.append(dsrc_cfg)
		return list_tasks_down

	def onDatCB_ReadPrep_impl(self, obj_datasrc, idx_step):
		#print("onDatCB_ReadPrep_impl ...")
		pass

	def onDatCB_ReadPost_impl(self, obj_datasrc, idx_step):
		print("onDatCB_ReadPost_impl ...")
		if self.obj_dset_trades != None and len(self.obj_dset_trades.loc_trades_recs) >  0:
			self.onDatCB_ReadPost_trades(obj_datasrc, idx_step)
		if self.obj_dset_candles != None and len(self.obj_dset_candles.loc_candle_recs) >  0:
			self.onDatCB_ReadPost_candles(obj_datasrc, idx_step)

	def onDatCB_ReadPost_trades(self, obj_datasrc, idx_step):
		print("onDatCB_ReadPost_trades", idx_step)
		num_trades = 0
		mts_last = None
		idx_sep  = None
		for idx_rec, rec_trades in enumerate(self.obj_dset_trades.loc_trades_recs):
			mts_rec  = rec_trades.get('mts', None)
			if mts_rec  == None:
				continue
			if   mts_last == None:
				mts_last = mts_rec
			elif mts_rec  > mts_last:
				idx_sep  = idx_rec
				break
		if idx_sep == None:
			return num_trades
		while len(self.obj_dset_trades.loc_trades_recs) >  idx_sep:
			rec_trades = self.obj_dset_trades.loc_trades_recs.pop(len(self.obj_dset_trades.loc_trades_recs)-1)
			self.down_rec_save.addRec_Trades(ktdata.DFMT_KKAIPRIV, rec_trades)
			num_trades += 1
		# update req range in data source
		if mts_last != None and obj_datasrc != None:
			obj_datasrc.setMts_ReqEnd(mts_last)
			print("New End:", mts_last)
		return num_trades

	def onDatCB_ReadPost_candles(self, obj_datasrc, idx_step):
		print("onDatCB_ReadPost_candles", idx_step)
		num_candles = 0
		mts_last = None
		while len(self.obj_dset_candles.loc_candle_recs) >  1:
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(len(self.obj_dset_candles.loc_candle_recs)-1)
			self.down_rec_save.addRec_Candles(ktdata.DFMT_KKAIPRIV, rec_candles)
			num_candles += 1
		if len(self.obj_dset_candles.loc_candle_recs) >  0:
			mts_last = self.obj_dset_candles.loc_candle_recs[0].get('mts', None)
		# update req range in data source
		if mts_last != None and obj_datasrc != None:
			obj_datasrc.setMts_ReqEnd(mts_last)
			print("New Candles End:", mts_last)
		return num_candles


class CTDataInput_HttpBfx_Down1(ktdata.CTDataInput_HttpBfx):
	def __init__(self, logger, obj_container, url_http_pref):
		ktdata.CTDataInput_HttpBfx.__init__(self, logger, obj_container, url_http_pref)
		self.loc_mark_end   = -1
		self.down_mts_end   = None

		self.flag_log_intv  = 1

	def setMts_ReqEnd(self, mts_end):
		self.down_mts_end   = mts_end

	def onMark_ChanEnd_impl(self):
		self.loc_mark_end   = self.tup_run_stat[1]

	def onMts_ReqRange_impl(self):
		if self.loc_mark_end == self.tup_run_stat[1]:
			return None
		print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl", self.tup_run_stat, self.obj_container.down_rec_save.rec_last_trades)
		if self.down_mts_end == None and (
			self.tup_run_stat[3] == 'trades' and self.tup_run_stat[5].get('symbol', None) == 'tBTCUSD'):
			rec_last = self.obj_container.down_rec_save.rec_last_trades
			print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl, trades rec_last:", rec_last)
			if rec_last != None:
				self.down_mts_end = rec_last.get('mts', None)
			if self.down_mts_end == None:
				self.down_mts_end = self.obj_container.down_rec_save.down_mts_end
		if self.down_mts_end == None and (
			self.tup_run_stat[3] == 'candles' and self.tup_run_stat[5].get('key', None) == 'trade:1m:tBTCUSD'):
			rec_last = self.obj_container.down_rec_save.rec_last_candles
			print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl, candles 1m:tBTCUSD rec_last:", rec_last)
			if rec_last != None:
				self.down_mts_end = rec_last.get('mts', None)
			if self.down_mts_end == None:
				self.down_mts_end = self.obj_container.down_rec_save.down_mts_end
		return None if self.down_mts_end == None else (-1, self.down_mts_end)


class CTDown1Rec_Save(object):
	def __init__(self, logger, mts_step=None):
		self.logger = logger
		self.inf_this = "CTDown1Rec_Save"
		self.rec_mts_step  = 60000 if mts_step == None else round(mts_step)
		self.down_mts_end  = 1519084800000
		self.flag_dbg_save =  0
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

		#self.flag_dbg_save =  2

	def dbInit(self, obj_container):
		return self.onDb_Init_impl(obj_container)

	def recReset(self):
		#self.onRec_Reset_impl(mts)
		pass

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
		str_db_name = 'bfx-down1'
		self.obj_dbwriter = ktdata.KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
		# create/open db table for trades
		cfg_chan = dsrc_http_trades['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_trades  = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_trades, map_chan['channel'], map_chan['wreq_args'])
		# create/open db table for candles
		cfg_chan = dsrc_http_candles['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_candles = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_candles, map_chan['channel'], map_chan['wreq_args'])
		# load last back record from database
		self.rec_last_trades  = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_trades, { }, [('$natural', -1)])
		self.rec_last_candles = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_candles, { }, [('$natural', -1)])
		return True

	def onRec_Reset_impl(self, mts):
		mts_this = math.floor(mts / self.rec_mts_step) * self.rec_mts_step
		flag_dup = True if self.mts_rec_this == mts_this else False
		self.mts_rec_this  = mts_this
		self.mts_rec_next  = self.mts_rec_this + self.rec_mts_step
		self.inf_this = "CTDown1Rec_Save(mts=" + format(self.mts_rec_this, ",") + ")"
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
		tid_last = None if self.rec_last_trades == None else self.rec_last_trades.get('tid', None)
		tid_this = rec_trades.get('tid', -1)
		if tid_last != None and tid_this >= tid_last:
			print("CTDown1Rec_Save::onAdd_RecTrades(Ign Rec) this:", rec_trades, ", last:", self.rec_last_trades)
		else:
			print("CTDown1Rec_Save::onAdd_RecTrades(Add Rec) this:", rec_trades, ", last:", self.rec_last_trades)
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_trades, rec_trades)
			self.rec_last_trades = rec_trades
		return True

	def onAdd_RecCandles(self, fmt_data, rec_candles):
		mts_last = None if self.rec_last_candles == None else self.rec_last_candles.get('mts', None)
		mts_this = rec_candles.get('mts', -1)
		#if self.flag_dbg_save >= 2:
		#	self.logger.debug(self.inf_this + ": add candles=" + str(rec_candles))
		if mts_last != None and mts_this >= mts_last:
			print("CTDown1Rec_Save::onAdd_RecCandles(Ign Rec) this:", rec_candles, ", last:", self.rec_last_candles)
		else:
			print("CTDown1Rec_Save::onAdd_RecCandles(Add Rec) this:", rec_candles, ", last:", self.rec_last_candles)
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_candles, rec_candles)
			self.rec_last_candles = rec_candles
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



