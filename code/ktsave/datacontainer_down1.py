
import json
import copy

import time

import urllib.parse

import ktdata

dsrc_http_trades = {
			#'switch': False,
			'url': 'https://api.bitfinex.com/v2',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', },
			],
		}

dsrc_http_candles = {
			#'switch': False,
			'url': 'https://api.bitfinex.com/v2',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', },
			],
		}

list_gap_trades  = []
list_gap_candles = []


class CTDataContainer_Down1Out(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.down_rec_save  = CTDown1Rec_Save(self.logger)

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
		obj_dataset = super(CTDataContainer_Down1Out, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		if   name_chan ==  'trades' and  self.obj_dset_trades == None:
			self.obj_dset_trades  = obj_dataset
		elif name_chan == 'candles' and self.obj_dset_candles == None:
			self.obj_dset_candles = obj_dataset
		return obj_dataset

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_Down1Out::onExec_Prep_impl(01)")
		self.run_loop_main    -= 1
		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		global list_gap_trades, list_gap_candles
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_Down1Out::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		for gap_idx, gap_trades  in enumerate(list_gap_trades):
			print("Trades  GAP(idx=" + str(gap_idx).zfill(3) + "):", gap_trades)
		for gap_idx, gap_candles in enumerate(list_gap_candles):
			print("Candles GAP(idx=" + str(gap_idx).zfill(3) + "):", gap_candles)

	def onDown_InitEntr_impl(self):
		self.down_rec_save.dbInit(self)
		return None

	def onDown_ExecCfg_init(self):
		#print("CTDataContainer_Down1Out::onDown_ExecCfg_init(00)")
		list_tasks_down = []
		if dsrc_http_trades.get('switch', True):
			dsrc_cfg = copy.copy(dsrc_http_trades)
			list_tasks_down.append(dsrc_cfg)
		if dsrc_http_candles.get('switch', True):
			dsrc_cfg = copy.copy(dsrc_http_candles)
			list_tasks_down.append(dsrc_cfg)
		return list_tasks_down

	def onDatCB_ReadPost_impl(self, obj_datasrc, idx_step):
		print("CTDataContainer_Down1Out::onDatCB_ReadPost_impl, setp:", idx_step)
		if self.obj_dset_trades != None and len(self.obj_dset_trades.loc_trades_recs) >  0:
			self.onDatCB_ReadPost_trades(obj_datasrc, idx_step)
		if self.obj_dset_candles != None and len(self.obj_dset_candles.loc_candle_recs) >  0:
			self.onDatCB_ReadPost_candles(obj_datasrc, idx_step)

	def onDatCB_ReadPost_trades(self, obj_datasrc, idx_step):
		print("CTDataContainer_Down1Out::onDatCB_ReadPost_trades, step:", idx_step)
		mark_finish = False
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
			ret_add = self.down_rec_save.addRec_Trades(ktdata.DFMT_KKAIPRIV, rec_trades)
			if ret_add == False:
				obj_datasrc.mark_ChanEnd()
				mark_finish =  True
				break
			num_trades += 1
		if mark_finish:
			return num_trades
		# update req range in data source
		if mts_last != None and obj_datasrc != None:
			obj_datasrc.setMts_ReqEnd(mts_last)
			print("CTDataContainer_Down1Out::onDatCB_ReadPost_trades, New End:", mts_last,
								time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(mts_last/1000))))
		return num_trades

	def onDatCB_ReadPost_candles(self, obj_datasrc, idx_step):
		print("CTDataContainer_Down1Out::onDatCB_ReadPost_candles, step:", idx_step)
		mark_finish = False
		num_candles = 0
		mts_last = None
		while len(self.obj_dset_candles.loc_candle_recs) >  1:
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(len(self.obj_dset_candles.loc_candle_recs)-1)
			ret_add = self.down_rec_save.addRec_Candles(ktdata.DFMT_KKAIPRIV, rec_candles)
			if ret_add == False:
				obj_datasrc.mark_ChanEnd()
				mark_finish =  True
				break
			num_candles += 1
		if mark_finish:
			return num_candles
		if len(self.obj_dset_candles.loc_candle_recs) >  0:
			mts_last = self.obj_dset_candles.loc_candle_recs[0].get('mts', None)
		# update req range in data source
		if mts_last != None and obj_datasrc != None:
			obj_datasrc.setMts_ReqEnd(mts_last)
			print("CTDataContainer_Down1Out::onDatCB_ReadPost_candles, New End:", mts_last,
								time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(mts_last/1000))))
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
		#print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl", self.tup_run_stat,
		#						self.obj_container.down_rec_save.rec_last_trades, self.obj_container.down_rec_save.rec_last_candles)
		if self.down_mts_end == None and (
			self.tup_run_stat[3] == 'trades' and self.tup_run_stat[5].get('symbol', None) == 'tBTCUSD'):
			rec_last = self.obj_container.down_rec_save.rec_last_trades
			print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl, trades(tBTCUSD) rec_last:", rec_last)
			if rec_last != None:
				self.down_mts_end = rec_last.get('mts', None)
			if self.down_mts_end == None:
				self.down_mts_end = self.obj_container.down_rec_save.down_mts_end
		if self.down_mts_end == None and (
			self.tup_run_stat[3] == 'candles' and self.tup_run_stat[5].get('key', None) == 'trade:1m:tBTCUSD'):
			rec_last = self.obj_container.down_rec_save.rec_last_candles
			print("CTDataInput_HttpBfx_Down1::onMts_ReqRange_impl, candles(1m:tBTCUSD) rec_last:", rec_last)
			if rec_last != None:
				self.down_mts_end = rec_last.get('mts', None)
			if self.down_mts_end == None:
				self.down_mts_end = self.obj_container.down_rec_save.down_mts_end
		return None if self.down_mts_end == None else (-1, self.down_mts_end)


class CTDown1Rec_Save(object):
	set_trades_excp  = { }
	set_candles_excp = { }
	#set_candles_excp = { 1516498800000, 1515733620000, 1514721960000, 1513090800000, 1511052480000, }

	def __init__(self, logger):
		self.logger = logger
		self.inf_this = "CTDown1Rec_Save"
		#self.down_mts_bgn = 1358182043000/1358548082000  Trades(Default)
		#self.down_mts_bgn = 1364774820000/1364827380000 Candles(Default)
		self.down_mts_bgn  = 1364947200000
		self.down_mts_end  = 1519084800000
		self.flag_dbg_save =  0
		# members for database
		self.obj_dbwriter  = None
		self.name_dbtbl_trades  = None
		self.name_dbtbl_candles = None
		self.rec_last_trades  = None
		self.rec_last_candles = None

		self.flag_dbg_save =  2

	def dbInit(self, obj_container):
		return self.onDb_Init_impl(obj_container)

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
		# load last download records from database
		self.rec_last_trades  = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_trades, { }, [('$natural', -1)])
		self.rec_last_candles = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_candles, { }, [('$natural', -1)])
		return True

	def onAdd_RecTrades(self, fmt_data, rec_trades):
		global list_gap_trades
		tid_last = None if self.rec_last_trades == None else self.rec_last_trades.get('tid', None)
		tid_this = rec_trades.get('tid', -1)
		mts_last = None if self.rec_last_trades == None else self.rec_last_trades.get('mts', None)
		mts_this = rec_trades.get('mts', -1)
		mts_diff = None if mts_last == None else (mts_last - mts_this)
		if mts_this <= self.down_mts_bgn:
			self.logger.warning(self.inf_this + " add Trades: record reach min MTS, rec: " + str(rec_trades))
			return False
		if mts_diff != None and mts_diff >  60000:
			self.logger.warning(self.inf_this + " add Trades: record mts GAP reach max, diff: " + str(mts_diff) +
								", trades=" + str(rec_trades) + ", last=" + str(self.rec_last_trades))
			if   mts_last in self.set_trades_excp:
				pass
			elif mts_diff <= 1800000:
				list_gap_trades.append((mts_last, mts_diff))
			else:
				return False
		if tid_last != None and tid_this >= tid_last:
			if self.flag_dbg_save >= 1:
				self.logger.warning(self.inf_this + " add Trades" +
								", ignore record=" + str(rec_trades) + ", last=" + str(self.rec_last_trades))
		else:
			if self.flag_dbg_save >= 2:
				self.logger.info(self.inf_this + " add Trades" +
								", new record=" + str(rec_trades) + ", last=" + str(self.rec_last_trades))
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_trades, rec_trades)
			self.rec_last_trades = rec_trades
		return True

	def onAdd_RecCandles(self, fmt_data, rec_candles):
		global list_gap_candles
		mts_last = None if self.rec_last_candles == None else self.rec_last_candles.get('mts', None)
		mts_this = rec_candles.get('mts', -1)
		mts_diff = None if mts_last == None else (mts_last - mts_this)
		if mts_this <= self.down_mts_bgn:
			self.logger.warning(self.inf_this + " add Candles: record reach min MTS, rec: " + str(rec_candles))
			return False
		if mts_diff != None and mts_diff >  60000:
			self.logger.warning(self.inf_this + " add Candles: record mts GAP reach max, diff: " + str(mts_diff) +
								", candles=" + str(rec_candles) + ", last= " + str(self.rec_last_candles))
			if   mts_last in self.set_candles_excp:
				pass
			elif mts_diff <= 1800000:
				list_gap_candles.append((mts_last, mts_diff))
			else:
				return False
		if mts_last != None and mts_this >= mts_last:
			if self.flag_dbg_save >= 1:
				self.logger.warning(self.inf_this + " add Candles" +
								", ignore record=" + str(rec_candles) + ", last=" + str(self.rec_last_candles))
		else:
			if self.flag_dbg_save >= 2:
				self.logger.info(self.inf_this + " add Candles" +
								", new record=" + str(rec_candles) + ", last=" + str(self.rec_last_candles))
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_candles, rec_candles)
			self.rec_last_candles = rec_candles
		return True


