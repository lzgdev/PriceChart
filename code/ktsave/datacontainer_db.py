
import json

import ktdata

class CTDataContainer_DbOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args):
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDbOut_Adapter_ticker(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'trades':
			obj_dataout = CTDbOut_Adapter_trades(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'book':
			obj_dataout = CTDbOut_Adapter_book(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'candles':
			obj_dataout = CTDbOut_Adapter_candles(self.logger, obj_dataset, self.obj_dbwriter)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		#print("CTDataContainer_DbOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		if obj_dataout != None:
			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		#print("CTDataContainer_DbOut::onDatCB_DataSync_impl", idx_chan)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.synAppend(msec_now)

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		#print("CTDataContainer_DbOut::onDatCB_RecPlus_impl", idx_chan, doc_rec, idx_rec)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.docAppend(doc_rec)


import time
import math
import copy

class CTDbOut_Adapter(ktdata.CTDataOutput):
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
		#def onPrep_OutChan_impl(self, name_dbtbl, name_chan, wreq_args):
		name_dbtbl = kwargs['name_dbtbl']
		name_chan  = kwargs['name_chan']
		wreq_args  = kwargs['wreq_args']
		# append collection to database
		if not self.loc_db_writer.dbOP_CollAdd(name_dbtbl, name_chan, wreq_args):
			return False
		self.name_dbtbl = name_dbtbl
		self.name_chan  = name_chan
		self.wreq_args  = wreq_args
		self.db_doc_last    = self.loc_db_writer.dbOP_DocFind_One(self.name_dbtbl, { }, [('_id', -1)])
		return True

	def onDocAppend_impl(self, doc_rec):
		# append doc to database
		doc_new  = self.loc_db_writer.dbOP_DocAdd(self.name_dbtbl, doc_rec)
		if doc_new != None:
			if self.flag_dbg_rec:
				self.logger.info("CTDbOut_Adapter(onSynAppend_impl): doc_new=" + str(doc_new))
			self.db_doc_last = doc_new
		return doc_new

class CTDbOut_Adapter_ticker(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer)


class CTDbOut_Adapter_trades(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer)

	def onSynAppend_impl(self, msec_now):
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDbOut_Adapter_trades(onSynAppend_impl): syn_rec=" + str(trade_rec))
			self.docAppend(trade_rec)

	def docAppend(self, doc_rec):
		if self.db_doc_last != None and doc_rec['tid'] <= self.db_doc_last['tid']:
			doc_new = None
		else:
			doc_new = self.onDocAppend_impl(doc_rec)
		return doc_new


class CTDbOut_Adapter_book(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer)
		self.num_recs_wrap  = 240
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0
		self.mts_sync_now   = None

	def docAppend(self, doc_rec):
		msec_now = doc_rec['mts']
		if (msec_now - self.mts_recs_last) >= 500:
			self.cnt_recs_book += 1
		flag_new_sync = True if self.cnt_recs_book >= self.num_recs_wrap else False
		if flag_new_sync:
			self.synAppend(msec_now)
		else:
			self.onDocAppend_impl(doc_rec)
			self.mts_recs_last  = msec_now

	def onSynAppend_impl(self, msec_now):
		if msec_now == None:
			msec_now = self.obj_dataset.loc_time_this
		self.mts_sync_now   = msec_now
		# add doc of reset
		out_doc = { 'type': 'reset', }
		self.onDocAppend_impl(out_doc)
		# add docs from snapshot
		for out_doc in self.obj_dataset.loc_book_bids:
			self.onDocAppend_impl(out_doc)
		for out_doc in self.obj_dataset.loc_book_asks:
			self.onDocAppend_impl(out_doc)
		# add doc of sync
		out_doc = { 'type': 'sync', }
		self.onDocAppend_impl(out_doc)
		# reset self.cnt_recs_book
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0
		self.mts_recs_last  = msec_now
		self.mts_sync_now   = None

	def onDocAppend_impl(self, doc_rec):
		out_doc = copy.copy(doc_rec)
		if self.mts_sync_now != None:
			out_doc['mts'] = self.mts_sync_now
		if 'sumamt' in out_doc:
			del out_doc['sumamt']
		return super(CTDbOut_Adapter_book, self).onDocAppend_impl(out_doc)


class CTDbOut_Adapter_candles(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer)
		self.doc_rec_output = None
		#self.flag_dbg_rec   = True

	def onSynAppend_impl(self, msec_now):
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDbOut_Adapter_candles(onSynAppend_impl): syn_rec=" + str(candle_rec))
			self.docAppend(candle_rec)

	def docAppend(self, doc_rec):
		msec_new = doc_rec['mts']
		if self.doc_rec_output != None and msec_new >  self.doc_rec_output['mts']:
			self.onDocAppend_impl(self.doc_rec_output)
			if self.db_doc_last != None and self.doc_rec_output['mts'] <= self.db_doc_last['mts']:
				doc_new = None
			else:
				doc_new = self.onDocAppend_impl(self.doc_rec_output)
		self.doc_rec_output = doc_rec

