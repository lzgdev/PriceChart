
import time
import math

from .dataset import CTDataSet_Ticker, CTDataSet_ABooks, CTDataSet_ACandles

def _eval_name_coll(mtime_utc):
	name_tmp = time.strftime("%Y%m%d%H%M", time.gmtime(mtime_utc / 1000))
	return name_tmp

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
		self.loc_date_dur   = 30 * 60 * 1000
		self.loc_name_coll  = None
		self.loc_date_next  = 0

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		utc_now = self.loc_time_this
		if (utc_now <  self.loc_date_next):
			# add update doc
			out_doc  = ticker_rec
			out_doc['mts'] = utc_now
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_Ticker_DbOut(onLocRecChg_CB): rec_new=" + str(out_doc))
			self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)
		else:
			# compose self.loc_name_coll
			utc_new = math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur
			self.loc_name_coll  = 'ticker-' + _eval_name_coll(utc_new)
			self.loc_date_next  = utc_new + self.loc_date_dur
			# add collection
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_Ticker_DbOut(onLocRecChg_CB): coll=" + self.loc_name_coll)
			self.loc_db_writer.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args)
			# add doc from snapshot
			out_doc  = ticker_rec
			out_doc['mts'] = utc_now
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_Ticker_DbOut(onLocRecChg_CB): rec_snp=" + str(out_doc))
			self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)


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

		self.loc_date_dur   = 30 * 60 * 1000
		self.loc_name_coll  = None
		self.loc_date_next  = 0

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		if not flag_sece:
			return
		utc_now = self.loc_time_this
		if utc_now <  self.loc_date_next:
			# add update doc
			out_doc  = book_rec
			out_doc['mts'] = utc_now
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_ABooks_DbOut(onLocRecChg_CB): rec_new=" + str(out_doc))
			self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)
		else:
			# compose self.loc_name_coll
			utc_new = math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur
			self.loc_name_coll  = 'book-' + self.wreq_args['prec'] + '-' + _eval_name_coll(utc_new)
			self.loc_date_next  = utc_new + self.loc_date_dur
			# add collection
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_ABooks_DbOut(onLocRecChg_CB): coll=" + self.loc_name_coll)
			self.loc_db_writer.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args)
			# add docs from snapshot
			for book_rec in self.loc_book_bids:
				out_doc  = book_rec
				out_doc['mts'] = utc_now
				if self.flag_dbg_rec:
					self.logger.info("CTDataSet_ABooks_DbOut(onLocRecChg_CB): rec_bid=" + str(out_doc))
				self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)
			for book_rec in self.loc_book_asks:
				out_doc  = book_rec
				out_doc['mts'] = utc_now
				if self.flag_dbg_rec:
					self.logger.info("CTDataSet_ABooks_DbOut(onLocRecChg_CB): rec_ask=" + str(out_doc))
				self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)

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
		self.flag_dbg_rec   = True
		self.loc_db_writer  = db_writer

		self.loc_date_dur   = 30 * 60 * 1000
		self.loc_name_coll  = None
		self.loc_date_next  = 0
		self.loc_mts_sync   = 0

		i1  =  self.wreq_args['key'].find(':')
		i2  =  -1 if i1 <  0 else self.wreq_args['key'].find(':', i1+1)
		self.loc_name_key   = '' if i1 < 0 or i2 < 0 else self.wreq_args['key'][i1+1 : i2]

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index):
		if not flag_sece:
			return
		if len(self.loc_candle_recs) <= 1:
			return
		candle_rec = self.loc_candle_recs[len(self.loc_candle_recs)-2]
		utc_now = candle_rec['mts']
		if utc_now <  self.loc_date_next and utc_now >  self.loc_mts_sync:
			# add update doc
			out_doc  = candle_rec
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_ACandles_DbOut(onLocRecChg_CB): rec_new=" + str(out_doc))
			self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)
		elif utc_now >= self.loc_date_next:
			# compose self.loc_name_coll
			utc_new  = math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur
			self.loc_name_coll  = 'candles-' + self.loc_name_key + '-' + _eval_name_coll(utc_new)
			self.loc_date_next  = utc_new + self.loc_date_dur
			# add collection
			if self.flag_dbg_rec:
				self.logger.info("CTDataSet_ACandles_DbOut(onLocRecChg_CB): coll=" + self.loc_name_coll)
			self.loc_db_writer.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args)
			# add docs from snapshot
			for out_idx, candle_rec in enumerate(self.loc_candle_recs):
				if candle_rec['mts'] >  utc_now:
					continue
				out_doc = candle_rec
				if self.flag_dbg_rec:
					self.logger.info("CTDataSet_ACandles_DbOut(onLocRecChg_CB): rec_snp=" + str(out_doc))
				self.loc_db_writer.dbOP_AddDoc(self.loc_name_coll, out_doc)
		# update self.loc_mts_sync
		self.loc_mts_sync   = utc_now


