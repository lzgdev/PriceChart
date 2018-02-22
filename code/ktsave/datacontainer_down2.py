
import json
import copy

import time

import urllib.parse

import ktdata

dsrc_down1_trades = {
			#'switch': False,
			'url': 'mongodb://127.0.0.1:27017/bfx-down1',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'limit': 256, 'sort': [('$natural', -1)], }, },
			],
		}

dsrc_down1_candles = {
			#'switch': False,
			'url': 'mongodb://127.0.0.1:27017/bfx-down1',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', 'load_args': { 'limit': 256, 'sort': [('$natural', -1)], }, },
			],
		}


class CTDataContainer_Down2Out(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.flag_down_finish  = False
		self.flag_down_trades  = True
		self.flag_down_candles = True

		self.down_rec_save  = CTDown2Rec_Save(self.logger)

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

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
		obj_dataset = super(CTDataContainer_Down2Out, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		if   name_chan ==  'trades' and  self.obj_dset_trades == None:
			self.obj_dset_trades  = obj_dataset
		elif name_chan == 'candles' and self.obj_dset_candles == None:
			self.obj_dset_candles = obj_dataset
		return obj_dataset

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_Down2Out::onExec_Prep_impl(01)")
		self.run_loop_main    -= 1
		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_Down2Out::onExec_Post_impl, time cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		if self.obj_dset_trades != None:
			self.onExec_Post_trades()
		if self.obj_dset_candles != None:
			self.onExec_Post_candles()
		self.flag_down_finish  = False if self.flag_down_trades or self.flag_down_candles else True

	def onDown_InitEntr_impl(self):
		self.down_rec_save.dbInit(self)
		return None

	def onDown_ExecCfg_init(self):
		#print("CTDataContainer_Down2Out::onDown_ExecCfg_init(00)")
		list_tasks_down = []
		if   not self.flag_down_trades:
			pass
		elif not dsrc_down1_trades.get('switch', True):
			self.flag_down_trades  = False
		else:
			dsrc_cfg = copy.copy(dsrc_down1_trades)
			rec_last = self.down_rec_save.rec_last_trades
			if rec_last != None:
				dsrc_cfg['chans'][0]['load_args']['filter'] = { 'tid': { '$gt': rec_last['tid'], } }
			list_tasks_down.append(dsrc_cfg)
		if   not self.flag_down_candles:
			pass
		elif not dsrc_down1_candles.get('switch', True):
			self.flag_down_candles = False
		else:
			dsrc_cfg = copy.copy(dsrc_down1_candles)
			rec_last = self.down_rec_save.rec_last_candles
			if rec_last != None:
				dsrc_cfg['chans'][0]['load_args']['filter'] = { 'mts': { '$gt': rec_last['mts'], } }
			list_tasks_down.append(dsrc_cfg)
		return list_tasks_down

	def onExec_Post_trades(self):
		print("CTDataContainer_Down2Out::onExec_Post_trades")
		num_trades = 0
		while len(self.obj_dset_trades.loc_trades_recs) >  0:
			rec_trades = self.obj_dset_trades.loc_trades_recs.pop(0)
			ret_add = self.down_rec_save.addRec_Trades(ktdata.DFMT_KKAIPRIV, rec_trades)
			if ret_add == False:
				break
			num_trades += 1
		self.obj_dset_trades.locDataClean()
		if num_trades == 0:
			self.flag_down_trades  = False
		return num_trades

	def onExec_Post_candles(self):
		print("CTDataContainer_Down2Out::onExec_Post_candles")
		num_candles = 0
		while len(self.obj_dset_candles.loc_candle_recs) >  0:
			rec_candles = self.obj_dset_candles.loc_candle_recs.pop(0)
			ret_add = self.down_rec_save.addRec_Candles(ktdata.DFMT_KKAIPRIV, rec_candles)
			if ret_add == False:
				break
			num_candles += 1
		self.obj_dset_candles.locDataClean()
		if num_candles == 0:
			self.flag_down_candles = False
		return num_candles


class CTDown2Rec_Save(object):
	def __init__(self, logger):
		self.logger = logger
		self.inf_this = "CTDown2Rec_Save"
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
		str_db_name = 'bfx-down'
		self.obj_dbwriter = ktdata.KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
		# create/open db table for trades
		cfg_chan = dsrc_down1_trades['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_trades  = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_trades, map_chan['channel'], map_chan['wreq_args'])
		# create/open db table for candles
		cfg_chan = dsrc_down1_candles['chans'][0]
		map_chan = obj_container._gmap_TaskChans_chan(cfg_chan['channel'], cfg_chan['wreq_args'])
		self.name_dbtbl_candles = map_chan['name_dbtbl']
		self.obj_dbwriter.dbOP_CollAdd(self.name_dbtbl_candles, map_chan['channel'], map_chan['wreq_args'])
		# load last download records from database
		self.rec_last_trades  = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_trades, { }, [('$natural', -1)])
		self.rec_last_candles = self.obj_dbwriter.dbOP_DocFind_One(self.name_dbtbl_candles, { }, [('$natural', -1)])
		return True

	def onAdd_RecTrades(self, fmt_data, rec_trades):
		ret_add = False
		tid_last = None if self.rec_last_trades == None else self.rec_last_trades.get('tid', None)
		tid_this = rec_trades.get('tid', -1)
		if tid_last != None and tid_this <= tid_last:
			if self.flag_dbg_save >= 1:
				self.logger.warning(self.inf_this + " add Trades" +
								", ignore record=" + str(rec_trades) + ", last=" + str(self.rec_last_trades))
		else:
			if self.flag_dbg_save >= 2:
				self.logger.info(self.inf_this + " add Trades" +
								", new record=" + str(rec_trades) + ", last=" + str(self.rec_last_trades))
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_trades, rec_trades)
			self.rec_last_trades = rec_trades
			ret_add =  True
		return ret_add

	def onAdd_RecCandles(self, fmt_data, rec_candles):
		ret_add = False
		mts_last = None if self.rec_last_candles == None else self.rec_last_candles.get('mts', None)
		mts_this = rec_candles.get('mts', -1)
		if mts_last != None and mts_this <= mts_last:
			if self.flag_dbg_save >= 1:
				self.logger.warning(self.inf_this + " add Candles" +
								", ignore record=" + str(rec_candles) + ", last=" + str(self.rec_last_candles))
		else:
			if self.flag_dbg_save >= 2:
				self.logger.info(self.inf_this + " add Candles" +
								", new record=" + str(rec_candles) + ", last=" + str(self.rec_last_candles))
			self.obj_dbwriter.dbOP_DocAdd(self.name_dbtbl_candles, rec_candles)
			self.rec_last_candles = rec_candles
			ret_add =  True
		return ret_add


