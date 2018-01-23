
import ktdata

class CTDataContainer_NetOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args):
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTNetOut_Adapter_ticker(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'trades':
			obj_dataout = CTNetOut_Adapter_trades(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'book':
			obj_dataout = CTNetOut_Adapter_book(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'candles':
			obj_dataout = CTNetOut_Adapter_candles(self.logger, obj_dataset, self.obj_dbwriter)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		#print("CTDataContainer_NetOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		if obj_dataout != None:
			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		#print("CTDataContainer_NetOut::onDatCB_DataSync_impl", idx_chan)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.synAppend(msec_now)

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		#print("CTDataContainer_NetOut::onDatCB_RecPlus_impl", idx_chan, doc_rec, idx_rec)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.docAppend(doc_rec)


import time
import math
import copy

class CTDataInput_DbReader(ktdata.CTDataInput_Db):
	def __init__(self, logger, obj_container):
		CTDataInput_Db.__init__(self, logger, obj_container)
		self.obj_dbadapter  = None
		self.flag_rec_plus  = True
		self.list_tmp_docs  = []

	def datFwd_Begin(self, id_chan):
		self.onDat_Begin_impl(id_chan)

	def datFwd_Doc(self, id_chan, obj_doc):
		self.onDat_FwdDoc_impl(id_chan, obj_doc)

	def datFwd_End(self, id_chan, num_docs):
		self.onDat_End_impl(id_chan, num_docs)

	def onInit_DbPrep_impl(self):
		return False

	def onExec_DbRead_impl(self):
		if self.obj_dbadapter != None:
			self.obj_dbadapter.dbOP_CollLoad(id_chan, name_coll, find_args, sort_args)
		"""
		self.obj_db_reader = None
		self.obj_db_reader = KTDataMedia_DbReader_WsOut(self.logger)
		self.obj_db_reader.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		obj_doc   = self.obj_db_reader.dbOP_DocFind_One(ktdata.COLLNAME_CollSet, filt_args, [('$nature', -1)])
		self.obj_db_reader.setDataset(obj_dataset)
		self.obj_db_reader.dbOP_CollLoad(id_chan_sbsc, name_coll, {}, None)
		"""

	def onDat_Begin_impl(self, id_chan):
		self.flag_rec_plus  = False
		self.list_tmp_docs.clear()

	def onDat_End_impl(self, id_chan, num_docs):
		self.flag_rec_plus  = False
		self.list_tmp_docs.clear()

	def onDat_FwdDoc_impl(self, id_chan, obj_doc):
		type_rec = None if not 'type' in obj_doc else obj_doc['type']
		if   type_rec == 'reset':
			self.flag_rec_plus  = False
			self.list_tmp_docs.clear()
		elif type_rec == 'sync':
			self.obj_container.datIN_DataFwd(id_chan, fmt_data, [id_chan, self.list_tmp_docs])
			self.list_tmp_docs.clear()
			self.flag_rec_plus  =  True
		else:
			if not self.flag_rec_plus:
				self.list_tmp_docs.append(obj_doc)
			else:
				self.obj_container.datIN_DataFwd(id_chan, fmt_data, obj_doc)


class KTDataMedia_DbReader_Adp(ktdata.KTDataMedia_DbReader):
	def __init__(self, logger, obj_dbreader):
		super(KTDataMedia_DbReader_WsOut, self).__init__(logger)
		self.obj_dbreader = obj_dbreader

	def onDbEV_CollLoad_Begin(self, id_chan):
		#self.logger.info("DataMedia(load): begin, chan=" + str(id_chan))
		self.obj_dbreader.datFwd_Begin(id_chan)

	def onDbEV_CollLoad_Doc(self, id_chan, obj_doc):
		#self.logger.info("DataMedia(load): chan=" + str(id_chan) + ", doc=" + str(obj_doc))
		self.obj_dbreader.datFwd_Doc(id_chan, obj_doc)

	def onDbEV_CollLoad_End(self, id_chan, num_docs):
		#self.logger.info("DataMedia(load): end, chan=" + str(id_chan) + ", num=" + str(num_docs))
		self.obj_dbreader.datFwd_End(id_chan, num_docs)


class CTNetOut_Adapter(ktdata.CTDataOutput):
	def __init__(self, logger, obj_dataset, db_writer):
		CTDataOutput.__init__(self, logger)
		self.obj_dataset    = obj_dataset
		self.loc_db_writer  = db_writer
		self.flag_dbg_rec   = False
		self.name_dbtbl = None
		self.name_chan  = None
		self.wreq_args  = None
		self.db_doc_last    = None

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
				self.logger.info("CTNetOut_Adapter(onSynAppend_impl): doc_new=" + str(doc_new))
			self.db_doc_last = doc_new
		return doc_new

class CTNetOut_Adapter_ticker(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, db_writer)


class CTNetOut_Adapter_trades(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, db_writer)

	def onSynAppend_impl(self, msec_now):
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTNetOut_Adapter_trades(onSynAppend_impl): syn_rec=" + str(trade_rec))
			self.docAppend(trade_rec)

	def docAppend(self, doc_rec):
		if self.db_doc_last != None and doc_rec['tid'] <= self.db_doc_last['tid']:
			doc_new = None
		else:
			doc_new = self.onDocAppend_impl(doc_rec)
		return doc_new


class CTNetOut_Adapter_book(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, db_writer)
		self.num_recs_wrap  = 240
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0

	def onSynAppend_impl(self, msec_now):
		if msec_now == None:
			msec_now = self.obj_dataset.loc_time_this
		# add doc of reset
		out_doc = { 'type': 'reset', 'mts': msec_now, }
		self.docAppend(out_doc)
		# add docs from snapshot
		for book_rec in self.obj_dataset.loc_book_bids:
			self.docAppend(book_rec)
		for book_rec in self.obj_dataset.loc_book_asks:
			self.docAppend(book_rec)
		# add doc of sync
		out_doc = { 'type': 'sync', 'mts': msec_now, }
		self.docAppend(out_doc)
		# reset self.cnt_recs_book
		self.cnt_recs_book  = 0
		self.mts_recs_last  = 0

	def docAppend(self, doc_rec):
		msec_now = doc_rec['mts']
		if (msec_now - self.mts_recs_last) >= 500:
			self.cnt_recs_book += 1
		flag_new_coll = True if msec_now >= self.msec_dbc_nxt else False
		flag_new_sync = True if self.cnt_recs_book >= self.num_recs_wrap else False
		if flag_new_coll or flag_new_sync:
			self.synAppend(msec_now)
		out_doc  = copy.copy(doc_rec)
		if 'sumamt' in out_doc:
			del out_doc['sumamt']
		if self.onDocAppend_impl(out_doc):
			self.mts_recs_last  = msec_now


class CTNetOut_Adapter_candles(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, db_writer)
		self.doc_rec_output = None
		#self.flag_dbg_rec   = True

	def onSynAppend_impl(self, msec_now):
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTNetOut_Adapter_candles(onSynAppend_impl): syn_rec=" + str(candle_rec))
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

