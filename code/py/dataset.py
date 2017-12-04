
class CTDataSet_Base(object):
	def __init__(self, name_chan, wreq_args):
		self.name_chan = name_chan
		self.chan_id = None
		self.wreq_args = wreq_args
		self.flag_loc_time  = False
		self.loc_time_this  = 0

	def locSet_ChanId(self, chan_id):
		self.chan_id = chan_id

	def locCleanData(self):
		self.onLocCleanData_impl()
		self.onLocCleanData_CB()

	def locAppendData(self, data_src, obj_msg):
		if (self.flag_loc_time):
			self.loc_time_this = Date.now()
		self.onLocAppendData_impl(data_src, obj_msg)
		self.onLocAppendData_CB(null)

	def locRecChg(self, flag_sece, data_src, obj_rec):
		self.onLocRecChg_impl(flag_sece, data_src, obj_rec)

	def onLocCleanData_CB(self):
		pass

	def onLocAppendData_CB(chan_data):
		pass

	def onLocCleanData_impl(self):
		pass

	def onLocAppendData_impl(self, data_src, obj_msg):
		pass

	def onLocRecChg_impl(self, flag_sece, data_src, obj_rec):
		pass

class CTDataSet_Array(CTDataSet_Base):
	def __init__(self, name_chan, wreq_args):
		super(CTDataSet_Array, self).__init__(name_chan, wreq_args)

	def onLocAppendData_impl(self, data_src, obj_msg):
		if (data_src == 1001):
			data_msg  = obj_msg['data']
			if not isinstance(data_msg, list):
				self.locRecChg(True, data_src, data_msg)
			else:
				self.locCleanData(data_msg)
				i_end = data_msg.length-1
				for (i=0; i <  i_end; i++):
					self.locRecChg(false, data_src, data_msg[i])
				if (i_end >= 0):
					self.locRecChg( true, data_src, data_msg[i])
		else:
			data_msg  = obj_msg[1]
			if not isinstance(data_msg, list):
				pass
			elif not isinstance(data_msg[0], list):
				self.locRecChg(True, data_src, data_msg)
			else:
				self.locCleanData(data_msg)
				i_end = data_msg.length-1
				for (i=0; i <  i_end; i++):
					self.locRecChg(false, data_src, data_msg[i])
				if (i_end >= 0):
					self.locRecChg( true, data_src, data_msg[i])

class CTDataSet_Ticker(CTDataSet_Base):
	def __init__(self, wreq_args):
		super(CTDataSet_Ticker, self).__init__('ticker', wreq_args)

	def onLocAppendData_impl(self, data_src, obj_msg):
		data_msg = obj_msg[1]
		if isinstance(data_msg, list) and len(data_msg) == 10:
			ticker_rec = {
					'bid':        Number(data_msg[0]),
					'bid_size':   Number(data_msg[1]),
					'ask':        Number(data_msg[2]),
					'ask_size':   Number(data_msg[3]),
					'daily_change':   Number(data_msg[4]),
					'daily_change_perc':  Number(data_msg[5]),
					'last_price': Number(data_msg[6]),
					'volume':     Number(data_msg[7]),
					'high':       Number(data_msg[8]),
					'low':        Number(data_msg[9]),
				}
			self.onLocRecChg_CB(ticker_rec, 0)

	def onLocRecChg_CB(ticker_rec, rec_index):
		pass


class CTDataSet_ABooks(CTDataSet_Array)
	def __init__(self, wreq_args):
		super(CTDataSet_ABooks, self).__init__("book", wreq_args)
		self.loc_book_bids  = []
		self.loc_book_asks  = []

	def onLocCleanData_impl(self):
		self.loc_book_bids.clear()
		self.loc_book_asks.clear()

	def onLocRecChg_impl(self, flag_sece, data_src, obj_rec):
		if (data_src == 1001):
			flag_bids  = (obj_rec['type'] == 'bid') ? True : False
			book_rec   = obj_rec
		else:
			flag_bids  = (obj_rec[2] >  0.0) ? True : False
			amount_rec = Number(flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]))
			book_rec   = {
					'price':  Number(obj_rec[0]),
					'type':   flag_bids ? 'bid' : 'ask',
					'count':  obj_rec[1],
					'amount': amount_rec,
					'sumamt': 0.0,
				}
		book_recs = flag_bids ? self.loc_book_bids : self.loc_book_asks
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		idx_bgn = 0
		idx_end = book_recs.length - 1
		while (idx_bgn < idx_end):
			idx_book  = Math.round((idx_bgn + idx_end) / 2)
			price_cmp = book_recs[idx_book].price
			if (book_rec.price <  price_cmp):
				idx_end = idx_book - 1
			elif (book_rec.price >  price_cmp):
				idx_bgn = idx_book + 1
			else:
				idx_bgn = idx_end = idx_book
		idx_book  = idx_end # now idx_book and idx_end < book_recs.length
		# delete/add/update book record in self.loc_book_bids or self.loc_book_asks
		flag_del  = (book_rec.count == 0) ? True : False
		if  (flag_del):
			if ((idx_book <  0) || (idx_book >= book_recs.length)):
				flag_del  = false
			else:
				book_recs.splice(idx_book, 1)
		elif ((idx_book <  0) || (book_rec.price >  book_recs[idx_book].price)):
			idx_book = idx_book + 1
			if ((idx_book <  0) || (idx_book >= book_recs.length)):
				book_recs.push(book_rec)
			else:
				book_recs.splice(idx_book, 0, book_rec)
		elif (book_rec.price <  book_recs[idx_book].price):
			if ((idx_book <  0) || (idx_book >= book_recs.length)):
				book_recs.push(book_rec)
			else:
				book_recs.splice(idx_book, 0, book_rec)
		else:
			book_recs.splice(idx_book, 1, book_rec)
		# update .sumamt in self.loc_book_bids or self.loc_book_asks
		idx_last  =  idx_book + (flag_bids ? 1 : -1)
		idx_last  = (idx_last <  0 || idx_last >= book_recs.length) ? -1 : idx_last
		for (idx_sum=idx_book; idx_sum >= 0 && idx_sum <  book_recs.length; idx_sum+=flag_bids ? -1 : 1):
			book_recs[idx_sum].sumamt = 0.0 + book_recs[idx_sum].amount +
						((idx_last <  0) ? 0.0 : book_recs[idx_last].sumamt)
			idx_last = idx_sum
		# invoke callback
		self.onLocRecChg_CB(flag_sece, flag_del ? book_rec : book_recs[idx_book],
                flag_bids, idx_book, flag_del)

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		pass

	# develop/debug support
	def devCheck_Books(self, err_ses)
		arr_errors = []
		strErr_pref = self.constructor.name + ' Err' +
				((err_ses == null || err_ses == '') ? ':' : ('(' + err_ses + '):'))
		# check book of bids
		for (idx_rec=0; idx_rec < self.loc_book_bids.length; idx_rec++):
			idx_other = idx_rec-1;
			if ((idx_other >= 0) && (idx_other <  self.loc_book_bids.length) &&
				(self.loc_book_bids[idx_rec].price <= self.loc_book_bids[idx_other].price)):
				arr_errors.push(strErr_pref + "(book bids disorder): "
					+ "len=" + self.loc_book_bids.length + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_bids[idx_rec]) +
					", last=" + JSON.stringify(self.loc_book_bids[idx_other]));
			idx_other = idx_rec-1;
			if ((idx_other >= 0) && (idx_other <  self.loc_book_bids.length) &&
				(Math.abs(sum_diff = (self.loc_book_bids[idx_other].sumamt - self.loc_book_bids[idx_other].amount -
					self.loc_book_bids[idx_rec].sumamt)) >  0.0001)):
				arr_errors.push(strErr_pref + "(book bids sum mistake): "
					+ "len=" + self.loc_book_bids.length + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_bids[idx_rec]) +
					", next=" + JSON.stringify(self.loc_book_asks[idx_other]) +
					", diff=" + sum_diff);
		# check book of asks
		for (idx_rec=0; idx_rec < self.loc_book_asks.length; idx_rec++):
			idx_other = idx_rec-1;
			if ((idx_other >= 0) && (idx_other < self.loc_book_asks.length) &&
				(self.loc_book_asks[idx_rec].price <= self.loc_book_asks[idx_other].price)):
				arr_errors.push(strErr_pref + "(book asks disorder): "
					+ "len=" + self.loc_book_asks.length + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_asks[idx_rec]) +
					", last=" + JSON.stringify(self.loc_book_asks[idx_other]));
			idx_other = idx_rec+1;
			if ((idx_other >= 0) && (idx_other < self.loc_book_asks.length) &&
				(Math.abs(sum_diff = (self.loc_book_asks[idx_other].sumamt - self.loc_book_asks[idx_other].amount -
					self.loc_book_asks[idx_rec].sumamt)) >  0.0001)):
				arr_errors.push(strErr_pref + "(book asks sum mistake): "
					+ "len=" + self.loc_book_asks.length + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_asks[idx_rec]) +
					", next=" + JSON.stringify(self.loc_book_asks[idx_other]) +
					", diff=" + sum_diff);
		return arr_errors

class CTDataSet_ACandles(CTDataSet_Array)
	def __init__(self, recs_size, wreq_args)
		super(CTDataSet_ACandles, self).__init__("candles", wreq_args);
		self.loc_recs_size   = recs_size
		self.loc_candle_recs = []

	def onLocCleanData_impl(self):
		self.loc_candle_recs.length = 0

	def onLocRecChg_impl(self, flag_sece, data_src, obj_rec):
		if (data_src == 1001):
			candle_rec = obj_rec;
		else:
			candle_rec = {
					'mts':    Number(obj_rec[0]),
					'open':   Number(obj_rec[1]),
					'close':  Number(obj_rec[2]),
					'high':   Number(obj_rec[3]),
					'low':    Number(obj_rec[4]),
					'volume': Number(obj_rec[5]),
				}
		flag_chg  = False
		for (rec_index=self.loc_candle_recs.length-1; rec_index >= 0; rec_index--):
			if (candle_rec.mts >= self.loc_candle_recs[rec_index].mts):
				break;
		if ((rec_index >= 0) && (self.loc_candle_recs[rec_index].mts == candle_rec.mts)):
			if (self.loc_candle_recs[rec_index].volume != candle_rec.volume):
				self.loc_candle_recs.splice(rec_index, 1, candle_rec)
				flag_chg  = true;
		else:
			if (self.loc_candle_recs.length+1 >  self.loc_recs_size):
				self.loc_candle_recs.pop();
			if ((rec_index <  0) || (candle_rec.mts >  self.loc_candle_recs[rec_index].mts)):
				rec_index ++;
			if (rec_index >= self.loc_candle_recs.length):
				self.loc_candle_recs.push(candle_rec)
			else:
				self.loc_candle_recs.splice(rec_index, 0, candle_rec);
			flag_chg  = true;
		if (flag_chg):
			self.onLocRecChg_CB(flag_sece, candle_rec, rec_index);

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index)
		pass

