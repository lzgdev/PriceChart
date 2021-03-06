
import time
import copy

DFMT_KKAIPRIV = 1001
DFMT_BFXV2    = 2001
MSEC_TIMEOFFSET = 0

def utTime_utcmts_now():
	return int(round(time.time() * 1000))

class CTDataSet_Base(object):
	flag_dbg_set = 0

	def __init__(self, logger, obj_container, name_chan, wreq_args):
		self.logger   = logger
		self.obj_container = obj_container
		self.name_chan = name_chan
		self.inf_this  = "DSet(" + self.name_chan + ")"
		self.name_dbtbl = None
		self.wreq_args = wreq_args
		self.id_chan   = None
		self.flag_sys_time  = False
		self.loc_time_this  = 0

	def locSet_DbTbl(self, name_dbtbl):
		self.name_dbtbl = name_dbtbl

	def locSet_ChanId(self, id_chan):
		self.id_chan = id_chan

	def locDataClean(self):
		self.onLocDataClean_impl()
		self.onCB_DataClean()
		self.obj_container.datCB_DataClean(self)

	def locDataSync(self):
		self.onLocDataSync_impl()
		self.onCB_DataSync()
		self.obj_container.datCB_DataSync(self, self.loc_time_this)

	def locDataAppend(self, fmt_data, obj_msg):
		if (self.flag_sys_time):
			self.loc_time_this = utTime_utcmts_now() + MSEC_TIMEOFFSET
		self.onLocDataAppend_impl(fmt_data, obj_msg)

	def locRecAdd(self, flag_plus, fmt_data, obj_rec):
		tup_add = self.onLocRecAdd_impl(flag_plus, fmt_data, obj_rec)
		if tup_add != None:
			self.onCB_RecAdd(flag_plus, tup_add[0], tup_add[1])
		if tup_add != None and flag_plus:
			self.obj_container.datCB_RecPlus(self, tup_add[0], tup_add[1])

	def onLocDataClean_impl(self):
		pass

	def onLocDataSync_impl(self):
		pass

	def onLocDataAppend_impl(self, fmt_data, obj_msg):
		pass

	def onLocRecAdd_impl(self, flag_plus, fmt_data, obj_rec):
		return None

	def onCB_DataClean(self):
		pass

	def onCB_DataSync(self):
		pass

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		pass


class CTDataSet_Array(CTDataSet_Base):
	def __init__(self, logger, obj_container, name_chan, wreq_args):
		super(CTDataSet_Array, self).__init__(logger, obj_container, name_chan, wreq_args)
		self.inf_this  = "DSetA(" + self.name_chan + ")"

	def onLocDataAppend_impl(self, fmt_data, obj_msg):
		data_msg   = None
		flag_plus  = False
		flag_array = False
		if   fmt_data == DFMT_KKAIPRIV:
			data_msg  = obj_msg
			if isinstance(data_msg, list):
				flag_array =  True
			else:
				flag_plus  =  True
		elif fmt_data == DFMT_BFXV2 and isinstance(obj_msg, list):
			data_msg = obj_msg[len(obj_msg)-1]
			if   isinstance(data_msg[0], list):
				flag_array =  True
			elif 'hb' == data_msg:
				flag_plus  = False
			else:
				flag_plus  =  True
		if self.flag_dbg_set >= 2:
			self.logger.info(self.inf_this + " onLocDataAppend_impl, data=" + str(data_msg) + ", msg=" + str(obj_msg))
		# append data to class data members
		if flag_plus:
			self.locRecAdd(True, fmt_data, data_msg)
		if flag_array:
			self.locDataClean()
			for idx_rec, obj_rec in enumerate(data_msg):
				self.locRecAdd(False, fmt_data, obj_rec)
			self.locDataSync()


class CTDataSet_Ticker(CTDataSet_Base):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_Ticker, self).__init__(logger, obj_container, 'ticker', wreq_args)
		self.flag_sys_time  = True
		self.loc_ticker_rec = None

	def onLocDataAppend_impl(self, fmt_data, obj_msg):
		flag_plus  = False
		ticker_rec = None
		if   (fmt_data == DFMT_KKAIPRIV):
			flag_plus  =  True
			ticker_rec = copy.copy(obj_msg)
		elif (fmt_data == DFMT_BFXV2):
			data_msg = obj_msg[1]
			msec_now = self.loc_time_this
			if 'hb' == data_msg:
				flag_plus  = False
				ticker_rec = None
			else:
				flag_plus  =  True
				try:
					ticker_rec = {
						'mts':        msec_now,
						'bid':        data_msg[0],
						'bid_size':   data_msg[1],
						'ask':        data_msg[2],
						'ask_size':   data_msg[3],
						'daily_change':   data_msg[4],
						'daily_change_perc':  data_msg[5],
						'last_price': data_msg[6],
						'volume':     data_msg[7],
						'high':       data_msg[8],
						'low':        data_msg[9],
						}
				except:
					ticker_rec = None
					self.logger.error("CTDataSet_Ticker(onLocRecAdd_impl): add obj_msg=" + str(obj_msg))
		if ticker_rec != None:
			self.loc_ticker_rec = ticker_rec
		return (ticker_rec, 0)


class CTDataSet_ATrades(CTDataSet_Array):
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ATrades, self).__init__(logger, obj_container, "trades", wreq_args)
		self.loc_recs_size   = recs_size
		self.loc_trades_recs = []

		#self.flag_dbg_set =  2

	def onLocDataClean_impl(self):
		self.loc_trades_recs.clear()

	def onLocRecAdd_impl(self, flag_plus, fmt_data, obj_rec):
		if self.flag_dbg_set >= 1:
			self.logger.info(self.inf_this + " onLocRecAdd_impl, obj_rec=" + str(obj_rec))
		if   (fmt_data == DFMT_KKAIPRIV):
			trade_rec = copy.copy(obj_rec)
		elif (fmt_data == DFMT_BFXV2):
			try:
				trade_rec = {
					'mts':    obj_rec[1],
					'tid':    obj_rec[0],
					'amount': obj_rec[2],
					'price':  obj_rec[3],
					}
			except:
				trade_rec = None
				self.logger.error(self.inf_this + " onLocRecAdd_impl(Error), add obj_rec=" + str(obj_rec))
		if (len(self.loc_trades_recs)+1 >  self.loc_recs_size):
			self.loc_trades_recs.pop(0)
		tid_new = trade_rec['tid']
		mts_new = trade_rec['mts']
		rec_index = len(self.loc_trades_recs) - 1
		while rec_index >= 0:
			if tid_new >= self.loc_trades_recs[rec_index]['tid']:
				break
			if mts_new >  self.loc_trades_recs[rec_index]['mts']:
				break
			rec_index -= 1
		if rec_index >= 0 and tid_new == self.loc_trades_recs[rec_index]['tid']:
			rec_index  = -1
		else:
			rec_index +=  1
		if rec_index >= 0:
			self.loc_trades_recs.insert(rec_index, trade_rec)
		return None if rec_index <  0 else (trade_rec, rec_index)


class CTDataSet_ABooks(CTDataSet_Array):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_ABooks, self).__init__(logger, obj_container, "book", wreq_args)
		self.flag_sys_time  = True
		self.loc_book_bids  = []
		self.loc_book_asks  = []

	def onLocDataClean_impl(self):
		self.loc_book_bids.clear()
		self.loc_book_asks.clear()

	def onLocRecAdd_impl(self, flag_plus, fmt_data, obj_rec):
		if   (fmt_data == DFMT_KKAIPRIV):
			flag_bids  = True if obj_rec['type'] == 'bid' else False
			book_rec   = copy.copy(obj_rec)
		elif (fmt_data == DFMT_BFXV2):
			msec_now   = self.loc_time_this
			try:
				flag_bids  = True if obj_rec[2] >  0.0 else False
				amount_rec = obj_rec[2] if flag_bids else (0.0 - obj_rec[2])
				book_rec   = {
						'mts':    msec_now,
						'price':  obj_rec[0],
						'type':   'bid' if flag_bids else 'ask',
						'count':  obj_rec[1],
						'amount': amount_rec,
						'sumamt': 0.0,
					}
			except:
				book_rec   = None
				self.logger.error("CTDataSet_ABooks(onLocRecAdd_impl): add obj_rec=" + str(obj_rec))
		book_recs = self.loc_book_bids if flag_bids else self.loc_book_asks
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		idx_bgn = 0
		idx_end = len(book_recs) - 1
		while (idx_bgn < idx_end):
			idx_book  = round((idx_bgn + idx_end) / 2)
			price_cmp = book_recs[idx_book]['price']
			if   (book_rec['price'] <  price_cmp):
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
		return (book_rec, idx_book)

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
			idx_other = idx_rec-1
			if ((idx_other >= 0) and (idx_other < len(self.loc_book_asks)) and
				(self.loc_book_asks[idx_rec].price <= self.loc_book_asks[idx_other].price)):
				arr_errors.push(strErr_pref + "(book asks disorder): "
					+ "len=" + len(self.loc_book_asks) + ",idx=" + idx_rec +
					", new=" + JSON.stringify(self.loc_book_asks[idx_rec]) +
					", last=" + JSON.stringify(self.loc_book_asks[idx_other]))
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
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ACandles, self).__init__(logger, obj_container, "candles", wreq_args)
		self.loc_recs_size   = recs_size
		self.loc_candle_recs = []

		#self.flag_dbg_set    =  2

	def onLocDataClean_impl(self):
		self.loc_candle_recs.clear()

	def onLocRecAdd_impl(self, flag_plus, fmt_data, obj_rec):
		if   (fmt_data == DFMT_KKAIPRIV):
			candle_rec = copy.copy(obj_rec)
		elif (fmt_data == DFMT_BFXV2):
			try:
				candle_rec = {
						'mts':    obj_rec[0],
						'open':   obj_rec[1],
						'close':  obj_rec[2],
						'high':   obj_rec[3],
						'low':    obj_rec[4],
						'volume': obj_rec[5],
					}
			except:
				candle_rec = None
				self.logger.error("CTDataSet_ACandles(onLocRecAdd_impl): add obj_rec=" + str(obj_rec))
		if (len(self.loc_candle_recs)+1 >  self.loc_recs_size):
			self.loc_candle_recs.pop(0)
		rec_index = len(self.loc_candle_recs) - 1
		while rec_index >= 0:
			if candle_rec['mts'] >= self.loc_candle_recs[rec_index]['mts']:
				break
			rec_index -= 1
		if ((rec_index >= 0) and (self.loc_candle_recs[rec_index]['mts'] == candle_rec['mts'])):
			# replace record in self.loc_candle_recs
			self.loc_candle_recs[rec_index] = candle_rec
		else:
			# insert record in self.loc_candle_recs
			if ((rec_index <  0) or (candle_rec['mts'] >  self.loc_candle_recs[rec_index]['mts'])):
				rec_index += 1
			self.loc_candle_recs.insert(rec_index, candle_rec)
		return (candle_rec, rec_index)


