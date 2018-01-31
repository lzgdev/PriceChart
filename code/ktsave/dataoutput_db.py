
import copy

import ktdata

class CTDataOut_Db(ktdata.CTDataOutput):
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

	def onOut_DatOne_impl(self, dat_one):
		# append doc to database
		doc_new  = self.loc_db_writer.dbOP_DocAdd(self.name_dbtbl, dat_one)
		if doc_new != None:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Db(onOut_DatOne_impl): doc_new=" + str(doc_new))
			self.db_doc_last = doc_new
		return doc_new



class CTDataOut_Db_ticker(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)

		#self.flag_dbg_rec   = True


class CTDataOut_Db_trades(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)

		#self.flag_dbg_rec   = True

	def onSynAppend_impl(self, msec_now):
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Db_trades(onSynAppend_impl): syn_rec=" + str(trade_rec))
			self.docAppend(trade_rec)

	def docAppend(self, doc_rec):
		if self.db_doc_last != None and doc_rec['tid'] <= self.db_doc_last['tid']:
			doc_new = None
		else:
			doc_new = self.onDocAppend_impl(doc_rec)
		return doc_new


class CTDataOut_Db_book(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)
		self.num_recs_wrap  = 240
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0
		self.mts_sync_now   = None

		#self.flag_dbg_rec   = True

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
		return super(CTDataOut_Db_book, self).onDocAppend_impl(out_doc)


class CTDataOut_Db_candles(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)
		self.doc_rec_output = None

		#self.flag_dbg_rec   = True

	def onSynAppend_impl(self, msec_now):
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDataOut_Db_candles(onSynAppend_impl): syn_rec=" + str(candle_rec))
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

