
from dataset import CTDataSet_Ticker, CTDataSet_ABooks, CTDataSet_ACandles

def _eval_name_coll(time_utc):
	date_str = new Date(time_utc).toISOString();
	name_tmp = date_str.slice( 0,  4) + date_str.slice( 5,  7) + date_str.slice( 8, 10) +
				date_str.slice(11, 13) + date_str.slice(14, 16);
	return name_tmp;

class CTDataSet_Ticker_DbIn(CTDataSet_Ticker):
	def __init__(self, db_reader, wreq_args):
		super(CTDataSet_Ticker_DbIn, self).__init__(wreq_args)
		self.flag_loc_time  = true;
		self.loc_db_reader  = db_reader;

	def onLocRecChg_CB(ticker_rec, rec_index)
		pass

class CTDataSet_Ticker_DbOut(CTDataSet_Ticker):
	def __init__(self, db_writter, wreq_args)
		super(CTDataSet_Ticker_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = true;
		self.loc_db_writter = db_writter;
		self.loc_date_dur   = 30 * 60 * 1000;
		self.loc_name_coll  = null;
		self.loc_date_next  = 0;

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		utc_now = self.loc_time_this;
		if (utc_now <  self.loc_date_next):
			#console.log("CTDataSet_Ticker_DbOut(onLocRecChg_CB) 11:", "coll:", self.loc_name_coll, "doc:", JSON.stringify(ticker_rec));
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'update',
					'time': utc_now,
					'data': ticker_rec,
				});
		else:
			#compose self.loc_name_coll
			utc_new = Math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur;
			self.loc_name_coll  = 'ticker-' + _eval_name_coll(utc_new);
			self.loc_date_next  = utc_new + self.loc_date_dur;
			#add collection and docs
			#console.log("CTDataSet_Ticker_DbOut(onLocRecChg_CB) 21:", "coll:", self.loc_name_coll, "docs:", JSON.stringify(ticker_rec));
			self.loc_db_writter.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args);
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'snapshot',
					'time': utc_now,
					'data': ticker_rec,
				})

class CTDataSet_ABooks_DbIn(CTDataSet_ABooks):
	def __init__(self, db_reader, wreq_args):
		super(CTDataSet_ABooks_DbIn, self).__init__(wreq_args)
		self.flag_loc_time  = true;
		self.loc_db_reader  = db_reader;

	def onLocRecChg_CB(flag_sece, book_rec, flag_bids, idx_book, flag_del):
		#console.log("CTDataSet_ABooks_DbIn(onLocRecChg_CB): rec:", JSON.stringify(book_rec));

class CTDataSet_ABooks_DbOut(CTDataSet_ABooks):
	def __init__(self, db_writter, wreq_args):
		super(CTDataSet_ABooks_DbOut, self).__init__(wreq_args)
		self.flag_loc_time  = true;
		self.loc_db_writter = db_writter;

		self.loc_date_dur   = 30 * 60 * 1000;
		self.loc_name_coll  = null;
		self.loc_date_next  = 0;

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		if (!flag_sece):
			return;
		utc_now = self.loc_time_this;
		if (utc_now <  self.loc_date_next):
			#console.log("CTDataSet_ABooks_DbOut(onLocRecChg_CB) 11:", "coll:", self.loc_name_coll, "doc:", JSON.stringify(book_rec));
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'update',
					'time': utc_now,
					'data': book_rec,
				});
		else:
			obj_docs = [];
			# compose obj_docs
			for (i=0; i < self.loc_book_bids.length; i++):
				obj_docs.push(self.loc_book_bids[i]);
			for (i=0; i < self.loc_book_asks.length; i++):
				obj_docs.push(self.loc_book_asks[i]);
			# compose self.loc_name_coll
			utc_new = Math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur;
			self.loc_name_coll  = 'book-' + self.wreq_args.prec + '-' + _eval_name_coll(utc_new);
			self.loc_date_next  = utc_new + self.loc_date_dur;
			#console.log("CTDataSet_ABooks_DbOut(onLocRecChg_CB) 21:", "coll:", self.loc_name_coll, "docs:", JSON.stringify(obj_docs));
			# add collection and docs
			self.loc_db_writter.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args);
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'snapshot',
					'time': utc_now,
					'data': obj_docs,
				});

class CTDataSet_ACandles_DbIn(CTDataSet_ACandles):
	def __init__(self, recs_size, db_reader, wreq_args):
		super(CTDataSet_ACandles_DbIn, self).__init__(recs_size, wreq_args);
		self.loc_db_reader  = db_reader;

	def onLocRecChg_CB(flag_sece, candle_rec, rec_index):
		pass

class CTDataSet_ACandles_DbOut(CTDataSet_ACandles):
	def __init__(self, recs_size, db_writter, wreq_args):
		super(CTDataSet_ACandles_DbOut, self).__init__(recs_size, wreq_args);
		self.loc_db_writter = db_writter;

		self.loc_date_dur   = 30 * 60 * 1000;
		self.loc_name_coll  = null;
		self.loc_date_next  = 0;
		self.loc_mts_sync   = 0;

		i1  =  self.wreq_args.key.indexOf(':');
		i2  = (i1 < 0) ? -1 : self.wreq_args.key.indexOf(':', i1+1);
		self.loc_name_key   = ((i1 < 0) || (i2 < 0)) ? '' : self.wreq_args.key.slice(i1+1, i2);

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index):
		if (!flag_sece):
			return;
		if (self.loc_candle_recs.length <= 1):
			return;
		candle_rec = self.loc_candle_recs[self.loc_candle_recs.length - 2];
		utc_now = candle_rec.mts;
		if ((utc_now <  self.loc_date_next) && (utc_now >  self.loc_mts_sync)):
			#console.log("CTDataSet_ACandles_DbOut(onLocRecChg_CB) 11:", "coll:", self.loc_name_coll, "doc:", JSON.stringify(candle_rec));
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'update',
					'time': utc_now,
					'data': candle_rec,
				});
			self.loc_mts_sync   = utc_now;
		elif (utc_now >= self.loc_date_next):
			# compose obj_docs
			obj_docs = self.loc_candle_recs.slice(0, self.loc_candle_recs.length-1);
			# compose self.loc_name_coll
			utc_new = Math.floor(utc_now / self.loc_date_dur) * self.loc_date_dur;
			self.loc_name_coll  = 'candles-' + self.loc_name_key + '-' + _eval_name_coll(utc_new);
			self.loc_date_next  = utc_new + self.loc_date_dur;
			#console.log("CTDataSet_ACandles_DbOut(onLocRecChg_CB) 21:", "coll:", self.loc_name_coll, "docs:", JSON.stringify(obj_docs));
			# add collection and docs
			self.loc_db_writter.dbOP_AddColl(self.loc_name_coll, self.name_chan, self.wreq_args);
			self.loc_db_writter.dbOP_AddDoc(self.loc_name_coll, {
					'type': 'snapshot',
					'time': utc_now,
					'data': obj_docs,
				});
			self.loc_mts_sync   = utc_now;

"""
class CTDataSet_Ticker_DbIn(CTDataSet_Ticker):
class CTDataSet_Ticker_DbOut(CTDataSet_Ticker):
class CTDataSet_ABooks_DbIn(CTDataSet_ABooks):
class CTDataSet_ABooks_DbOut(CTDataSet_ABooks):
class CTDataSet_ACandles_DbIn(CTDataSet_ACandles):
class CTDataSet_ACandles_DbOut(CTDataSet_ACandles):
"""

