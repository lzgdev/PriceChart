
#
# obsolete code
#

from .dataset       import CTDataSet_Ticker, CTDataSet_ATrades, CTDataSet_ABooks, CTDataSet_ACandles

class CTDataSet_Ticker_DbOut(CTDataSet_Ticker):
	def __init__(self, logger, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_Ticker_DbOut, self).__init__(logger, wreq_args)
		self.loc_db_adapter = CTDbOut_Adapter(logger, db_writer, num_coll_msec,
						self.name_chan, self.wreq_args, 'ticker')

	def onLocRecAdd_CB(self, flag_plus, ticker_rec, rec_index):
		self.loc_db_adapter.docAppend(ticker_rec)

class CTDataSet_ATrades_DbOut(CTDataSet_ATrades):
	def __init__(self, logger, db_writer, num_coll_msec, recs_size, wreq_args):
		super(CTDataSet_ATrades_DbOut, self).__init__(recs_size, logger, wreq_args)
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

class CTDataSet_ABooks_DbOut(CTDataSet_ABooks):
	def __init__(self, logger, db_writer, num_coll_msec, wreq_args):
		super(CTDataSet_ABooks_DbOut, self).__init__(logger, wreq_args)
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

class CTDataSet_ACandles_DbOut(CTDataSet_ACandles):
	def __init__(self, logger, db_writer, num_coll_msec, recs_size, wreq_args):
		super(CTDataSet_ACandles_DbOut, self).__init__(recs_size, logger, wreq_args)
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

