
import os

import websocket

import hmac
import hashlib
import json

#from .datacontainer_db import CTDataSet_Ticker_DbOut, CTDataSet_ATrades_DbOut, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbOut
#from ktdata import CTDataSet_Ticker_DbOut, CTDataSet_ATrades_DbOut, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbOut

from .datainput import CTDataInput_Ws

from .dataset   import CTDataSet_Ticker_Adapter, CTDataSet_ATrades_Adapter, CTDataSet_ABooks_Adapter, CTDataSet_ACandles_Adapter
from .dataset   import DFMT_KKAIPRIV, DFMT_BITFINEX, MSEC_TIMEOFFSET

class CTDataContainer(object):
	def __init__(self, logger):
		object.__init__(self)
		self.logger   = logger
		self.pid_this = os.getpid()
		self.list_objs_datasrc = []

	def execLoop(self):
		self.onExec_Loop_impl()

	def addArg_DataChannel(self, name_chan, dict_args_wreq, tmp_tokchan):
		obj_netclient = self.list_objs_datasrc[0]
		#{ 'channel':  'ticker', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', }, },

		num_coll_msec  =  3 * 60 * 60 * 1000
		#num_coll_msec  =  1 * 60 * 60 * 1000

		obj_chan  = None
		#self.logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))
		if   name_chan == 'ticker':
			obj_chan = CTDataSet_Ticker_DbOut(self.logger, self, self.obj_dbwriter, num_coll_msec, dict_args_wreq)
		elif name_chan == 'trades':
			obj_chan = CTDataSet_ATrades_DbOut(self.logger, self, self.obj_dbwriter, num_coll_msec, 512, dict_args_wreq)
		elif name_chan == 'book':
			obj_chan = CTDataSet_ABooks_DbOut(self.logger, self, self.obj_dbwriter, num_coll_msec, dict_args_wreq)
		elif name_chan == 'candles':
			obj_chan = CTDataSet_ACandles_DbOut(self.logger, self, self.obj_dbwriter, num_coll_msec, 512, dict_args_wreq)

		if obj_chan != None:
			obj_netclient.addObj_DataReceiver(obj_chan, tmp_tokchan)

	def addObj_DataSource(self, obj_source):
		self.list_objs_datasrc.append(obj_source)

	def datCB_DataClean(self, obj_dataset):
		pass

	def datCB_DataSync(self, obj_dataset):
		pass

	def datCB_RecAdd(self, obj_dataset, flag_plus, doc_rec, idx_rec):
		pass

	def onExec_Loop_impl(self):
		for obj_source in self.list_objs_datasrc:
			if isinstance(obj_source, CTDataInput_Ws):
				obj_source.run_forever()

import time
import math
import copy

class CTDbOut_Adapter:
	def __init__(self, logger, db_writer, num_coll_msec, name_chan, wreq_args, name_pref):
		self.logger   = logger
		self.loc_db_writer  = db_writer
		self.num_coll_msec  = num_coll_msec
		self.name_chan  = name_chan
		self.wreq_args  = wreq_args
		self.flag_dbg_rec   = False
		self.msec_dbc_pre   = 0
		self.msec_dbc_nxt   = 0
		self.loc_name_pref  = name_pref
		self.loc_name_coll  = None

	def colAppend(self, msec_now):
		# re-asign self.msec_dbc_pre and self.msec_dbc_nxt
		msec_coll = math.floor(msec_now / self.num_coll_msec) * self.num_coll_msec
		self.msec_dbc_pre  = msec_coll - 1
		self.msec_dbc_nxt  = msec_coll + self.num_coll_msec
		# compose self.loc_name_coll
		self.loc_name_coll  = self.loc_name_pref + ('-' +
				time.strftime("%Y%m%d%H%M%S", time.gmtime(msec_coll / 1000)))
		# append collection to database
		self.loc_db_writer.dbOP_CollAdd(self.loc_name_coll, self.name_chan, self.wreq_args)
		doc_one  = self.loc_db_writer.dbOP_DocFind_One(self.loc_name_coll, { }, [('_id', -1)])
		if doc_one != None:
			self.msec_dbc_pre  = doc_one['mts']
		if self.flag_dbg_rec:
			self.logger.info("CTDbOut_Adapter(colAppend): coll=" + self.loc_name_coll +
								", msec: pre=" + str(self.msec_dbc_pre) + " nxt=" + str(self.msec_dbc_nxt))

	def docAppend(self, doc_rec):
		msec_doc = doc_rec['mts']
		# append collection to database
		if msec_doc >= self.msec_dbc_nxt:
			self.colAppend(msec_doc)
		# append doc to database
		if msec_doc <= self.msec_dbc_pre  or msec_doc >= self.msec_dbc_nxt:
			if self.flag_dbg_rec:
				self.logger.warning("CTDbOut_Adapter(docAppend): ignore doc=" + str(doc_rec))
			return False
		if self.flag_dbg_rec:
			self.logger.info("CTDbOut_Adapter(docAppend): new doc=" + str(doc_rec))
		self.loc_db_writer.dbOP_DocAdd(self.loc_name_coll, doc_rec)
		return True


class CTDataSet_Ticker_DbOut(CTDataSet_Ticker_Adapter):
	def __init__(self, logger, obj_container, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_Ticker_DbOut, self).__init__(logger, obj_container, wreq_args)
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'ticker')

	def onLocRecAdd_CB(self, flag_plus, ticker_rec, rec_index):
		self.loc_db_adapter.docAppend(ticker_rec)

class CTDataSet_ATrades_DbOut(CTDataSet_ATrades_Adapter):
	def __init__(self, logger, obj_container, db_writer, num_coll_msec, recs_size, wreq_args):
		super(CTDataSet_ATrades_DbOut, self).__init__(recs_size, logger, obj_container, wreq_args)
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'trades')

	def onLocDataSync_CB(self):
		for trade_rec in self.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_ATrades_DbOut(onLocDataSync_CB): rec=" + str(trade_rec))
			self.loc_db_adapter.docAppend(trade_rec)

	def onLocRecAdd_CB(self, flag_plus, trade_rec, rec_index):
		if not flag_plus:
			return 0
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ATrades_DbOut(onLocRecAdd_CB): rec=" + str(trade_rec))
		self.loc_db_adapter.docAppend(trade_rec)

class CTDataSet_ABooks_DbOut(CTDataSet_ABooks_Adapter):
	def __init__(self, logger, obj_container, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_ABooks_DbOut, self).__init__(logger, obj_container, wreq_args)
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'book-' + self.wreq_args['prec'])
		self.num_recs_wrap  = 240
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0

	def onLocDataSync_CB(self):
		msec_now = self.loc_time_this
		# add doc of reset
		out_doc = { 'type': 'reset', 'mts': msec_now, }
		self.loc_db_adapter.docAppend(out_doc)
		# add docs from snapshot
		for book_rec in self.loc_book_bids:
			out_doc  = copy.copy(book_rec)
			del out_doc['sumamt']
			self.loc_db_adapter.docAppend(out_doc)
		for book_rec in self.loc_book_asks:
			out_doc  = copy.copy(book_rec)
			del out_doc['sumamt']
			self.loc_db_adapter.docAppend(out_doc)
		# add doc of sync
		out_doc = { 'type': 'sync', 'mts': msec_now, }
		self.loc_db_adapter.docAppend(out_doc)
		# reset self.cnt_recs_book
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0

	def onLocRecAdd_CB(self, flag_plus, book_rec, flag_bids, idx_book, flag_del):
		if not flag_plus:
			return
		msec_now = self.loc_time_this
		if (msec_now - self.mts_recs_last) >= 500:
			self.cnt_recs_book += 1
		flag_new_coll = True if msec_now >= self.loc_db_adapter.msec_dbc_nxt else False
		flag_new_sync = True if self.cnt_recs_book >= self.num_recs_wrap else False
		if flag_new_coll:
			self.loc_db_adapter.colAppend(msec_now)
		if flag_new_coll or flag_new_sync:
			self.onLocDataSync_CB()
		out_doc  = copy.copy(book_rec)
		del out_doc['sumamt']
		if self.loc_db_adapter.docAppend(out_doc):
			self.mts_recs_last  = msec_now

def _extr_cname_key(wreq_key):
	i1  =  wreq_key.find(':')
	i2  =  -1 if i1 <  0 else wreq_key.find(':', i1+1)
	name_key = '' if i1 < 0 or i2 < 0 else wreq_key[i1+1 : i2]
	return name_key

class CTDataSet_ACandles_DbOut(CTDataSet_ACandles_Adapter):
	def __init__(self, logger, obj_container, db_writer, num_coll_msec, recs_size, wreq_args):
		super(CTDataSet_ACandles_DbOut, self).__init__(recs_size, logger, obj_container, wreq_args)
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'candles-' + _extr_cname_key(self.wreq_args['key']))
		self.loc_out_stamp  = 0

	def onLocDataClean_CB(self):
		self.loc_out_stamp  = 0

	def onLocDataSync_CB(self):
		for idx_rec in range(0, len(self.loc_candle_recs)-1):
			candle_rec = self.loc_candle_recs[idx_rec]
			if self.loc_db_adapter.docAppend(candle_rec):
				self.loc_out_stamp  = candle_rec['mts']

	def onLocRecAdd_CB(self, flag_plus, candle_rec, rec_index):
		if not flag_plus:
			return False
		idx_rec = len(self.loc_candle_recs) - 2
		if idx_rec <  0:
			return False
		candle_rec = self.loc_candle_recs[idx_rec]
		if candle_rec['mts'] <= self.loc_out_stamp:
			return False
		if self.loc_db_adapter.docAppend(candle_rec):
			self.loc_out_stamp  = candle_rec['mts']
		return True

