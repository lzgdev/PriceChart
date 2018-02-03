
import time
import math
import copy

import ktdata

class CTDataOut_Stat(ktdata.CTDataOutput):
	def __init__(self, logger, obj_dataset, db_writer):
		ktdata.CTDataOutput.__init__(self, logger)
		self.obj_dataset    = obj_dataset
		self.loc_db_writer  = db_writer
		self.flag_dbg_rec   = False
		self.name_dbtbl = None
		self.name_chan  = None
		self.wreq_args  = None
		self.db_doc_last    = None

	def getDoc_OutLast(self):
		return self.db_doc_last

	def onPrep_OutChan_impl(self, **kwargs):
		#print("CTDataOut_Stat::onPrep_OutChan_impl", kwargs)
		name_dbtbl = kwargs['name_dbtbl']
		name_chan  = kwargs['name_chan']
		wreq_args  = kwargs['wreq_args']
		"""
		# append collection to database
		if not self.loc_db_writer.dbOP_CollAdd(name_dbtbl, name_chan, wreq_args):
			return False
		self.name_dbtbl = name_dbtbl
		self.name_chan  = name_chan
		self.wreq_args  = wreq_args
		self.db_doc_last    = self.loc_db_writer.dbOP_DocFind_One(self.name_dbtbl, { }, [('_id', -1)])
		"""
		return True

	def onDocAppend_impl(self, doc_rec):
		mts_doc = doc_rec['mts']
		print("CTDataOut_Stat::onDocAppend_impl, mts:", format(mts_doc, ","), ", doc:", doc_rec)
		dat_unit = self.tranDoc2Dat(doc_rec)
		dat_sent = None
		self.outDatOne(dat_unit)
		if self.flag_dbg_rec:
			self.logger.info("CTDataOut_Stat(onSynAppend_impl): dat_unit=" + str(dat_unit) + ", doc_rec=" + str(dat_sent))
		return dat_unit



class CTDataOut_Stat_stat01(CTDataOut_Stat):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Stat.__init__(self, logger, obj_dataset, db_writer)
		#self.flag_dbg_rec = True

	def onSynAppend_impl(self, msec_now):
		dat_array = []
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Stat_stat01(onSynAppend_impl): syn_rec=" + str(candle_rec))
			dat_array.append(self.tranDoc2Dat(candle_rec))
		self.outDatArray(dat_array)


class CTDataOut_Stat_ticker(CTDataOut_Stat):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Stat.__init__(self, logger, obj_dataset, db_writer)
		#self.flag_dbg_rec = True


class CTDataOut_Stat_trades(CTDataOut_Stat):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Stat.__init__(self, logger, obj_dataset, db_writer)
		#self.flag_dbg_rec = True

	def onSynAppend_impl(self, msec_now):
		dat_array = []
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Stat_trades(onSynAppend_impl): syn_rec=" + str(trade_rec))
			dat_array.append(self.tranDoc2Dat(trade_rec))
		self.outDatArray(dat_array)


class CTDataOut_Stat_book(CTDataOut_Stat):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Stat.__init__(self, logger, obj_dataset, db_writer)
		#self.flag_dbg_rec = True

	def onSynAppend_impl(self, msec_now):
		# add docs from snapshot
		dat_array = []
		for book_rec in self.obj_dataset.loc_book_bids:
			dat_array.append(self.tranDoc2Dat(book_rec))
		for book_rec in self.obj_dataset.loc_book_asks:
			dat_array.append(self.tranDoc2Dat(book_rec))
		self.outDatArray(dat_array)


class CTDataOut_Stat_candles(CTDataOut_Stat):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Stat.__init__(self, logger, obj_dataset, db_writer)
		#self.flag_dbg_rec = True

	def onSynAppend_impl(self, msec_now):
		dat_array = []
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Stat_candles(onSynAppend_impl): syn_rec=" + str(candle_rec))
			dat_array.append(self.tranDoc2Dat(candle_rec))
		self.outDatArray(dat_array)


