
import time
import math

from .dataset import CTDataSet_Ticker, CTDataSet_ABooks, CTDataSet_ACandles

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

		self.flag_dbg_rec   = False

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
		doc_one  = self.loc_db_writer.dbOP_DocFind_One(self.loc_name_coll, { }, [('mts', -1)])
		if doc_one != None:
			self.msec_dbc_pre  = doc_one['mts']
		if self.flag_dbg_rec:
			self.logger.info("CTDbOut_Adapter(colAppend): coll=" + self.loc_name_coll +
								", msec: pre=" + str(self.msec_dbc_pre) + " nxt=" + str(self.msec_dbc_nxt))

	def docAppend(self, doc_rec):
		msec_doc = doc_rec['mts']
		# append collection to database
		if self.msec_dbc_nxt == 0  or msec_doc >= self.msec_dbc_nxt:
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

class CTDataSet_Ticker_DbIn(CTDataSet_Ticker):
	def __init__(self, logger, db_reader, wreq_args):
		super(CTDataSet_Ticker_DbIn, self).__init__(wreq_args)
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_reader  = db_reader

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_Ticker_DbIn(onLocRecChg_CB): rec=" + str(ticker_rec))

class CTDataSet_Ticker_DbOut(CTDataSet_Ticker):
	def __init__(self, logger, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_Ticker_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = True
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'ticker')

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		msec_now = self.loc_time_this
		out_doc  = ticker_rec
		out_doc['mts'] = msec_now
		self.loc_db_adapter.docAppend(out_doc)

class CTDataSet_ABooks_DbIn(CTDataSet_ABooks):
	def __init__(self, logger, db_reader, wreq_args):
		super(CTDataSet_ABooks_DbIn, self).__init__(wreq_args)
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_reader  = db_reader

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ABooks_DbIn(onLocRecChg_CB): rec=" + str(book_rec))


class CTDataSet_ABooks_DbOut(CTDataSet_ABooks):
	def __init__(self, logger, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_ABooks_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = True
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'book-' + self.wreq_args['prec'])
		self.num_recs_wrap  = 240
		self.cnt_recs_book  = 0

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		if not flag_sece:
			return
		msec_now = self.loc_time_this
		if self.cnt_recs_book % self.num_recs_wrap != 0:
			out_doc = book_rec
			out_doc['mts'] = msec_now
			self.loc_db_adapter.docAppend(out_doc)
		else:
			# add docs from snapshot
			out_doc = { 'type': 'reset', 'mts': msec_now, }
			self.loc_db_adapter.docAppend(out_doc)
			for book_rec in self.loc_book_bids:
				out_doc  = book_rec
				out_doc['mts'] = msec_now
				self.loc_db_adapter.docAppend(out_doc)
			for book_rec in self.loc_book_asks:
				out_doc  = book_rec
				out_doc['mts'] = msec_now
				self.loc_db_adapter.docAppend(out_doc)
			out_doc = { 'type': 'sync', 'mts': msec_now, }
		self.cnt_recs_book += 1

def _extr_cname_key(wreq_key):
	i1  =  wreq_key.find(':')
	i2  =  -1 if i1 <  0 else wreq_key.find(':', i1+1)
	name_key = '' if i1 < 0 or i2 < 0 else wreq_key[i1+1 : i2]
	return name_key

class CTDataSet_ACandles_DbIn(CTDataSet_ACandles):
	def __init__(self, logger, db_reader, recs_size, wreq_args):
		super(CTDataSet_ACandles_DbIn, self).__init__(recs_size, wreq_args)
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_reader  = db_reader

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ACandles_DbIn(onLocRecChg_CB): rec=" + str(candle_rec))

class CTDataSet_ACandles_DbOut(CTDataSet_ACandles):
	def __init__(self, logger, db_writer, num_coll_msec, recs_size, wreq_args):
		super(CTDataSet_ACandles_DbOut, self).__init__(recs_size, wreq_args)
		self.logger   = logger
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'candles-' + _extr_cname_key(self.wreq_args['key']))
		self.loc_out_count  = 0

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index):
		if not flag_sece:
			return
		if len(self.loc_candle_recs) <= 1:
			return
		idx_bgn = 0 if self.loc_out_count == 0 else len(self.loc_candle_recs) - 2
		for idx_rec in range(idx_bgn, len(self.loc_candle_recs) - 1):
			if idx_rec <  0:
				continue
			candle_rec = self.loc_candle_recs[idx_rec]
			if self.loc_db_adapter.docAppend(candle_rec):
				self.loc_out_count += 1

