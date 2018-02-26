
import copy

import ktdata

class CTDataOut_Db(ktdata.CTDataOutput):
	def __init__(self, logger, obj_dataset, db_writer):
		ktdata.CTDataOutput.__init__(self, logger)
		self.obj_dataset    = obj_dataset
		self.loc_db_writer  = db_writer
		self.name_dbtbl = None
		self.name_chan  = None
		self.wreq_args  = None
		self.db_doc_last    = None

		#self.flag_dbg_out  = 2

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
		self.db_doc_last    = self.loc_db_writer.dbOP_DocFind_One(self.name_dbtbl, { }, [('$natural', -1)])
		return True

	def onOut_DatOne_impl(self, dat_one):
		num_out = 0
		# append doc to database
		doc_new  = self.loc_db_writer.dbOP_DocAdd(self.name_dbtbl, dat_one)
		if doc_new != None:
			if self.flag_dbg_out >= 2:
				self.logger.info("CTDataOut_Db(onOut_DatOne_impl): doc_new=" + str(doc_new))
			self.db_doc_last = doc_new
			num_out  = 1
		return num_out

	def onOut_DatArray_impl(self, dat_array):
		num_out = 0
		for dat_out in dat_array:
			doc_new  = self.loc_db_writer.dbOP_DocAdd(self.name_dbtbl, dat_out)
			if doc_new != None:
				if self.flag_dbg_out >= 2:
					self.logger.info("CTDataOut_Db(onOut_DatArray_impl): doc_new=" + str(doc_new))
				self.db_doc_last = doc_new
				num_out += 1
		return num_out


class CTDataOut_Db_ticker(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)

		#self.flag_dbg_out   = 2


class CTDataOut_Db_trades(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)

		#self.flag_dbg_out   = 2

	def onSynAppend_get_list(self, msec_now):
		return self.obj_dataset.loc_trades_recs

	def onTran_Doc2Dat_impl(self, doc_rec):
		tid_last = None if self.db_doc_last == None else self.db_doc_last.get('tid', None)
		tid_this = doc_rec.get('tid', -1)
		if tid_last != None and tid_this <= tid_last:
			self.logger.info("CTDataOut_Db_trades(onTran_Doc2Dat_impl): ignore doc_rec=" + str(doc_rec) + ", doc_last=" + str(self.db_doc_last))
			return None
		return doc_rec


class CTDataOut_Db_book(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)
		self.mts_sync_now   = None
		self.mts_sync_next  = None

		#self.flag_dbg_out   = 2

	def docAppend(self, doc_rec):
		mts_this  = doc_rec.get('mts', 0)
		if self.mts_sync_next == None or mts_this >= self.mts_sync_next:
			return self.synAppend(mts_this)
		else:
			return super(CTDataOut_Db_book, self).docAppend(doc_rec)

	def onSynAppend_get_list(self, msec_now):
		list_books = []
		# add doc of reset
		out_doc = { 'type': 'reset', }
		list_books.append(out_doc)
		# add docs from snapshot
		list_books.extend(self.obj_dataset.loc_book_bids)
		list_books.extend(self.obj_dataset.loc_book_asks)
		# add doc of sync
		out_doc = { 'type': 'sync', }
		list_books.append(out_doc)
		return list_books

	def onSynAppend_impl(self, msec_now):
		if msec_now == None:
			msec_now = self.obj_dataset.loc_time_this
		self.mts_sync_now   = msec_now
		# call super class's method
		ret_super = super(CTDataOut_Db_book, self).onSynAppend_impl(msec_now)
		# reset self.mts_sync_now and self.self.mts_sync_next
		self.mts_sync_now   = None
		self.mts_sync_next  = msec_now + 600000
		return ret_super

	def onTran_Doc2Dat_impl(self, doc_rec):
		doc_out = copy.copy(doc_rec)
		if self.mts_sync_now != None:
			doc_out['mts'] = self.mts_sync_now
		if 'sumamt' in doc_out:
			del doc_out['sumamt']
		return doc_out


class CTDataOut_Db_candles(CTDataOut_Db):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOut_Db.__init__(self, logger, obj_dataset, db_writer)

		#self.flag_dbg_out   = 2

	def onSynAppend_get_list(self, msec_now):
		return self.obj_dataset.loc_candle_recs

	def onTran_Doc2Dat_impl(self, doc_rec):
		mts_last = None if self.db_doc_last == None else self.db_doc_last.get('mts', None)
		mts_this = doc_rec.get('mts', -1)
		if mts_last != None and mts_this <= mts_last:
			self.logger.info("CTDataOut_Db_candles(onTran_Doc2Dat_impl): ignore doc_rec=" + str(doc_rec) + ", doc_last=" + str(self.db_doc_last))
			return None
		return doc_rec


