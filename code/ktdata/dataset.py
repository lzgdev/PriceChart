
import time

from decimal import Decimal

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
			self.loc_time_this = int(round(time.time() * 1000))
		self.onLocAppendData_impl(data_src, obj_msg)
		self.onLocAppendData_CB(None)

	def locRecChg(self, flag_sece, data_src, obj_rec):
		self.onLocRecChg_impl(flag_sece, data_src, obj_rec)

	def onLocCleanData_CB(self):
		pass

	def onLocAppendData_CB(self, chan_data):
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
				self.locCleanData()
				idx_end = len(data_msg) - 1
				for rec_idx, rec_data in enumerate(data_msg):
					self.locRecChg(False if rec_idx < idx_end else True,
								data_src, rec_data)
		else:
			data_msg  = obj_msg[1]
			if not isinstance(data_msg, list):
				pass
			elif not isinstance(data_msg[0], list):
				self.locRecChg(True, data_src, data_msg)
			else:
				self.locCleanData()
				idx_end = len(data_msg) - 1
				for rec_idx, rec_data in enumerate(data_msg):
					self.locRecChg(False if rec_idx < idx_end else True,
								data_src, rec_data)

class CTDataSet_Ticker(CTDataSet_Base):
	def __init__(self, wreq_args):
		super(CTDataSet_Ticker, self).__init__('ticker', wreq_args)

	def onLocAppendData_impl(self, data_src, obj_msg):
		data_msg = obj_msg[1]
		if isinstance(data_msg, list) and len(data_msg) == 10:
			ticker_rec = {
					'bid':        Decimal(data_msg[0]),
					'bid_size':   Decimal(data_msg[1]),
					'ask':        Decimal(data_msg[2]),
					'ask_size':   Decimal(data_msg[3]),
					'daily_change':   Decimal(data_msg[4]),
					'daily_change_perc':  Decimal(data_msg[5]),
					'last_price': Decimal(data_msg[6]),
					'volume':     Decimal(data_msg[7]),
					'high':       Decimal(data_msg[8]),
					'low':        Decimal(data_msg[9]),
				}
			self.onLocRecChg_CB(ticker_rec, 0)

	def onLocRecChg_CB(self, ticker_rec, rec_index):
		pass


class CTDataSet_ABooks(CTDataSet_Array):
	def __init__(self, wreq_args):
		super(CTDataSet_ABooks, self).__init__("book", wreq_args)
		self.loc_book_bids  = []
		self.loc_book_asks  = []

	def onLocCleanData_impl(self):
		self.loc_book_bids.clear()
		self.loc_book_asks.clear()

	def onLocRecChg_impl(self, flag_sece, data_src, obj_rec):
		if (data_src == 1001):
			flag_bids  = True if obj_rec['type'] == 'bid' else False
			book_rec   = obj_rec
		else:
			flag_bids  = True if obj_rec[2] >  0.0 else False
			amount_rec = obj_rec[2] if flag_bids else (0.0 - obj_rec[2])
			book_rec   = {
					'price':  Decimal(obj_rec[0]),
					'type':   'bid' if flag_bids else 'ask',
					'count':  int(obj_rec[1]),
					'amount': amount_rec,
					'sumamt': 0.0,
				}
		book_recs = self.loc_book_bids if flag_bids else self.loc_book_asks
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		idx_bgn = 0
		idx_end = len(book_recs) - 1
		while (idx_bgn < idx_end):
			idx_book  = round((idx_bgn + idx_end) / 2)
			price_cmp = book_recs[idx_book]['price']
			if (book_rec['price'] <  price_cmp):
				idx_end = idx_book - 1
			elif (book_rec['price'] >  price_cmp):
				idx_bgn = idx_book + 1
			else:
				idx_bgn = idx_end = idx_book
		idx_book  = idx_end # now idx_book and idx_end < len(book_recs)
		# delete/add/update book record in self.loc_book_bids or self.loc_book_asks
		flag_del  = True if book_rec['count'] == 0 else False
		if  (flag_del):
			if ((idx_book <  0) or (idx_book >= len(book_recs))):
				flag_del  = False
			else:
				book_recs.pop(idx_book)
		elif ((idx_book <  0) or (book_rec['price'] >  book_recs[idx_book]['price'])):
			idx_book = idx_book + 1
			book_recs.insert(idx_book, book_rec)
		elif (book_rec['price'] <  book_recs[idx_book]['price']):
			book_recs.insert(idx_book, book_rec)
		else:
			book_recs[idx_book] = book_rec
		# update .sumamt in self.loc_book_bids or self.loc_book_asks
		idx_last  =  idx_book + (1 if flag_bids else -1)
		idx_last  =  -1 if (idx_last <  0 or idx_last >= len(book_recs)) else idx_last
		idx_sum   = idx_book
		while idx_sum >= 0 and idx_sum <  len(book_recs):
			book_recs[idx_sum]['sumamt']  = 0.0 + book_recs[idx_sum]['amount'] + (
						0.0 if (idx_last <  0) else book_recs[idx_last]['sumamt'])
			idx_last = idx_sum
			idx_sum += -1 if flag_bids else 1
		# invoke callback
		self.onLocRecChg_CB(flag_sece, book_rec if flag_del else book_recs[idx_book],
                flag_bids, idx_book, flag_del)

	def onLocRecChg_CB(self, flag_sece, book_rec, flag_bids, idx_book, flag_del):
		pass

	# develop/debug support
	def devCheck_Books(self, err_ses):
		arr_errors = []
		strErr_pref = self.constructor.name + ' Err' + (':'
				if (err_ses == None or err_ses == '') else '(' + err_ses + '):')
		# check book of bids
		idx_rec = 0
		while idx_rec < len(self.loc_book_bids):
			idx_other = idx_rec-1
			if ((idx_other >= 0) and (idx_other <  len(self.loc_book_bids)) and
				(self.loc_book_bids[idx_rec].price <= self.loc_book_bids[idx_other].price)):
				arr_errors.append(strErr_pref + "(book bids disorder): "
					+ "len=" + len(self.loc_book_bids) + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_bids[idx_rec]) +
					", last=" + JSON.stringify(self.loc_book_bids[idx_other]))
			idx_other = idx_rec-1
			if ((idx_other >= 0) and (idx_other <  len(self.loc_book_bids)) and
				(abs(sum_diff = (self.loc_book_bids[idx_other]['sumamt'] - self.loc_book_bids[idx_other]['amount'] -
					self.loc_book_bids[idx_rec]['sumamt'])) >  0.0001)):
				arr_errors.push(strErr_pref + "(book bids sum mistake): "
					+ "len=" + len(self.loc_book_bids) + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_bids[idx_rec]) +
					", next=" + JSON.stringify(self.loc_book_asks[idx_other]) +
					", diff=" + sum_diff)
			idx_rec += 1
		# check book of asks
		idx_rec = 0
		while idx_rec < len(self.loc_book_asks):
			idx_other = idx_rec-1;
			if ((idx_other >= 0) and (idx_other < len(self.loc_book_asks)) and
				(self.loc_book_asks[idx_rec].price <= self.loc_book_asks[idx_other].price)):
				arr_errors.push(strErr_pref + "(book asks disorder): "
					+ "len=" + len(self.loc_book_asks) + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_asks[idx_rec]) +
					", last=" + JSON.stringify(self.loc_book_asks[idx_other]));
			idx_other = idx_rec+1
			if ((idx_other >= 0) and (idx_other < len(self.loc_book_asks)) and
				(abs(sum_diff = (self.loc_book_asks[idx_other]['sumamt'] - self.loc_book_asks[idx_other]['amount'] -
					self.loc_book_asks[idx_rec]['sumamt'])) >  0.0001)):
				arr_errors.push(strErr_pref + "(book asks sum mistake): "
					+ "len=" + len(self.loc_book_asks) + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_asks[idx_rec]) +
					", next=" + JSON.stringify(self.loc_book_asks[idx_other]) +
					", diff=" + sum_diff)
			idx_rec += 1
		return arr_errors

class CTDataSet_ACandles(CTDataSet_Array):
	def __init__(self, recs_size, wreq_args):
		super(CTDataSet_ACandles, self).__init__("candles", wreq_args);
		self.loc_recs_size   = recs_size
		self.loc_candle_recs = []

	def onLocCleanData_impl(self):
		self.loc_candle_recs.clear()

	def onLocRecChg_impl(self, flag_sece, data_src, obj_rec):
		if (data_src == 1001):
			candle_rec = obj_rec;
		else:
			candle_rec = {
					'mts':    int(obj_rec[0]),
					'open':   Decimal(obj_rec[1]),
					'close':  Decimal(obj_rec[2]),
					'high':   Decimal(obj_rec[3]),
					'low':    Decimal(obj_rec[4]),
					'volume': Decimal(obj_rec[5]),
				}
		flag_chg  = False
		rec_index = len(self.loc_candle_recs) - 1;
		while rec_index >= 0:
			if candle_rec['mts'] >= self.loc_candle_recs[rec_index]['mts']:
				break;
			rec_index -= 1
		if ((rec_index >= 0) and (self.loc_candle_recs[rec_index]['mts'] == candle_rec['mts'])):
			if (self.loc_candle_recs[rec_index]['volume'] != candle_rec['volume']):
				self.loc_candle_recs[rec_index] = candle_rec
				flag_chg  = True
		else:
			if (len(self.loc_candle_recs)+1 >  self.loc_recs_size):
				self.loc_candle_recs.pop(0);
			if ((rec_index <  0) or (candle_rec['mts'] >  self.loc_candle_recs[rec_index]['mts'])):
				rec_index += 1
			self.loc_candle_recs.insert(rec_index, candle_rec)
			flag_chg  = True
		if (flag_chg):
			self.onLocRecChg_CB(flag_sece, candle_rec, rec_index);

	def onLocRecChg_CB(self, flag_sece, candle_rec, rec_index):
		pass

