

from .datacontainer import CTDataContainer

class CTDataContainer_DbOut(CTDataContainer):
	def __init__(self, logger):
		CTDataContainer.__init__(self, logger)
		self.list_dbad_dataout = []

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		#obj_dataset.onLocDataSync_CB()
		#print("CTDataContainer_DbOut::onDatCB_DataSync_impl", idx_chan)
		obj_dbad = self.list_dbad_dataout[idx_chan]
		if obj_dbad != None:
			obj_dbad.synAppend(msec_now)

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		#obj_dataset.onLocRecAdd_CB(True, doc_rec, idx_rec)
		#print("CTDataContainer_DbOut::onDatCB_RecPlus_impl", idx_chan, doc_rec, idx_rec)
		obj_dbad = self.list_dbad_dataout[idx_chan]
		if obj_dbad != None:
			obj_dbad.docAppend(doc_rec)


	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		tup_chan  = self.list_tups_datachn[idx_chan]
		obj_dataset = tup_chan[0]
		dict_args   = tup_chan[2]
		num_coll_msec  =  3 * 60 * 60 * 1000
		#num_coll_msec  =  1 * 60 * 60 * 1000

		obj_dbad = None
		if   name_chan == 'ticker':
			obj_dbad = CTDbOut_Adapter_ticker(self.logger, obj_dataset, self.obj_dbwriter, num_coll_msec,
								name_chan, dict_args)
		elif name_chan == 'trades':
			obj_dbad = CTDbOut_Adapter_trades(self.logger, obj_dataset, self.obj_dbwriter, num_coll_msec,
								name_chan, dict_args)
		elif name_chan == 'book':
			obj_dbad = CTDbOut_Adapter_book(self.logger, obj_dataset, self.obj_dbwriter, num_coll_msec,
								name_chan, dict_args)
		elif name_chan == 'candles':
			obj_dbad = CTDbOut_Adapter_candles(self.logger, obj_dataset, self.obj_dbwriter, num_coll_msec,
								name_chan, dict_args)
		# add database adapter to self.list_dbad_dataout
		while len(self.list_dbad_dataout) <= idx_chan:
			self.list_dbad_dataout.append(None)
		if obj_dbad != None:
			self.list_dbad_dataout[idx_chan] = obj_dbad


	def onDatIN_ChanDel_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		pass


import time
import math
import copy

class CTDbOut_Adapter(object):
	def __init__(self, logger, obj_dataset, db_writer, num_coll_msec, name_chan, wreq_args, name_pref):
		self.logger   = logger
		self.obj_dataset    = obj_dataset
		self.loc_db_writer  = db_writer
		self.num_coll_msec  = num_coll_msec
		self.name_chan  = name_chan
		self.wreq_args  = wreq_args
		self.flag_dbg_rec   = False
		self.msec_dbc_pre   = 0
		self.msec_dbc_nxt   = 0
		self.loc_name_pref  = name_pref
		self.loc_name_coll  = None

	def synAppend(self, msec_now):
		self.onSynAppend_impl(msec_now)

	def colAppend(self, msec_now):
		self.onColAppend_impl(msec_now)

	def docAppend(self, doc_rec):
		self.onDocAppend_impl(doc_rec)

	def onSynAppend_impl(self, msec_now):
		pass

	def onColAppend_impl(self, msec_now):
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

	def onDocAppend_impl(self, doc_rec):
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


class CTDbOut_Adapter_ticker(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer, num_coll_msec, name_chan, wreq_args):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer, num_coll_msec,
								name_chan, wreq_args, name_chan)


class CTDbOut_Adapter_trades(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer, num_coll_msec, name_chan, wreq_args):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer, num_coll_msec,
								name_chan, wreq_args, name_chan)

	def onSynAppend_impl(self, msec_now):
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDbOut_Adapter_trades(onSynAppend_impl): rec=" + str(trade_rec))
			self.docAppend(trade_rec)


class CTDbOut_Adapter_book(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer, num_coll_msec, name_chan, wreq_args):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer, num_coll_msec,
								name_chan, wreq_args, name_chan + '-' + wreq_args['prec'])
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
		if flag_new_coll:
			self.colAppend(msec_now)
		if flag_new_coll or flag_new_sync:
			self.synAppend(msec_now)
		out_doc  = copy.copy(doc_rec)
		if 'sumamt' in out_doc:
			del out_doc['sumamt']
		if self.onDocAppend_impl(out_doc):
			self.mts_recs_last  = msec_now


def _extr_cname_key(wreq_key):
	i1  =  wreq_key.find(':')
	i2  =  -1 if i1 <  0 else wreq_key.find(':', i1+1)
	name_key = '' if i1 < 0 or i2 < 0 else wreq_key[i1+1 : i2]
	return name_key

class CTDbOut_Adapter_candles(CTDbOut_Adapter):
	def __init__(self, logger, obj_dataset, db_writer, num_coll_msec, name_chan, wreq_args):
		CTDbOut_Adapter.__init__(self, logger, obj_dataset, db_writer, num_coll_msec,
								name_chan, wreq_args, name_chan + '-' + _extr_cname_key(wreq_args['key']))
		self.doc_rec_output = None

	def onSynAppend_impl(self, msec_now):
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTDbOut_Adapter_candles(onSynAppend_impl): rec=" + str(candle_rec))
			self.docAppend(candle_rec)

	def docAppend(self, doc_rec):
		msec_new = doc_rec['mts']
		if self.doc_rec_output != None and msec_new >  self.doc_rec_output['mts']:
			self.onDocAppend_impl(self.doc_rec_output)
		self.doc_rec_output = doc_rec


