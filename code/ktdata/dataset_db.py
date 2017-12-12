
import time
import math

from .dataset import CTDataSet_Ticker, CTDataSet_ABooks, CTDataSet_ACandles

def _eval_name_coll(mtime_utc):
	name_tmp = time.strftime("%Y%m%d%H%M%S", time.gmtime(mtime_utc / 1000))
	return name_tmp

class CTDbOut_Adapter:
	def __init__(self, logger, db_writer, name_chan, wreq_args, name_pref, mts_num):
		self.name_chan  = name_chan
		self.wreq_args  = wreq_args
		self.logger   = logger
		self.loc_db_writer  = db_writer
		self.flag_dbg_rec   = True
		self.stat_fin = False
		self.mts_num  = mts_num
		self.mts_bgn  = 0
		self.mts_end  = 0
		self.mts_dbc  = 0
		self.mts_nxt  = 0
		self.loc_name_pref  = name_pref
		self.loc_name_coll  = None

	def colAppend(self, mts_now):
		# re-asign self.mts_dbc and self.mts_nxt
		mts_new = math.floor(mts_now / self.mts_num) * self.mts_num
		self.mts_dbc  = mts_new
		self.mts_bgn  = mts_new
		self.mts_nxt  = mts_new + self.mts_num
		# compose self.loc_name_coll
		self.loc_name_coll  = self.loc_name_pref + ('-' +
				time.strftime("%Y%m%d%H%M%S", time.gmtime(self.mts_dbc / 1000)))
		# append collection to database
		if self.flag_dbg_rec:
			self.logger.info("CTDbOut_Adapter(colAppend): coll=" + self.loc_name_coll)
		self.loc_db_writer.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args)

	def docAppend(self, doc_rec):
		flag_add_col = False
		flag_add_doc = False
		if self.stat_fin:
			return flag_add_doc
		mts_doc = doc_rec['mts']
		if self.mts_bgn >  0 and mts_doc <  self.mts_bgn:
			return False
		if self.mts_end >  0 and mts_doc >= self.mts_end:
			self.stat_fin = True
			return False
		# append collection to database
		if self.mts_nxt == 0  or mts_doc >= self.mts_nxt:
			self.colAppend(mts_doc)
		# append doc to database
		if mts_doc <  self.mts_dbc  or mts_doc >= self.mts_nxt:
			return False
		if self.flag_dbg_rec:
			self.logger.info("CTDbOut_Adapter(docAppend): rec_snp=" + str(doc_rec))
		self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, doc_rec)
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
	def __init__(self, logger, db_writer, wreq_args):
		super(CTDataSet_Ticker_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = True
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_writer  = db_writer
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, self.name_chan, self.wreq_args,
						'ticker', 30*60*1000)

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		mts_now = self.loc_time_this
		out_doc  = ticker_rec
		out_doc['mts'] = mts_now
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
	def __init__(self, logger, db_writer, wreq_args):
		super(CTDataSet_ABooks_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = True
		self.logger   = logger
		self.flag_dbg_rec   = True
		self.loc_db_writer  = db_writer
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, self.name_chan, self.wreq_args,
						'book-' + self.wreq_args['prec'], 30*60*1000)

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		if not flag_sece:
			return
		mts_now = self.loc_time_this
		if   self.loc_db_adapter.mts_nxt >  0 and mts_now <  self.loc_db_adapter.mts_nxt:
			out_doc = book_rec
			out_doc['mts'] = mts_now
			self.loc_db_adapter.docAppend(out_doc)
		elif self.loc_db_adapter.mts_end <= 0  or mts_now <  self.loc_db_adapter.mts_end:
			# add docs from snapshot
			for book_rec in self.loc_book_bids:
				out_doc  = book_rec
				out_doc['mts'] = mts_now
				self.loc_db_adapter.docAppend(out_doc)
			for book_rec in self.loc_book_asks:
				out_doc  = book_rec
				out_doc['mts'] = mts_now
				self.loc_db_adapter.docAppend(out_doc)

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
	def __init__(self, logger, db_writer, recs_size, wreq_args):
		super(CTDataSet_ACandles_DbOut, self).__init__(recs_size, wreq_args)
		self.logger   = logger
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, self.name_chan, self.wreq_args,
						'candles-' + _extr_cname_key(self.wreq_args['key']), 30*60*1000)
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
			elif self.loc_db_adapter.stat_fin:
				self.flag_loc_term  = True

