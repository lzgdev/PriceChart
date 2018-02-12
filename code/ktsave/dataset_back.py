
import ktdata

class CTDataSet_Ticker_back(ktdata.CTDataSet_Ticker):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_Ticker_back, self).__init__(logger, obj_container, wreq_args)
		self.back_rec_last  = None

	def onCB_DataClean(self):
		self.back_rec_last  = None
		return super(CTDataSet_Ticker_back, self).onCB_DataClean()

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		return super(CTDataSet_Ticker_back, self).onCB_RecAdd(flag_plus, obj_rec, idx_rec)


class CTDataSet_ATrades_back(ktdata.CTDataSet_ATrades):
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ATrades_back, self).__init__(logger, recs_size, obj_container, wreq_args)
		self.back_rec_last  = None

	def onCB_DataClean(self):
		self.back_rec_last  = None
		return super(CTDataSet_ATrades_back, self).onCB_DataClean()

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		num_trades = len(self.loc_trades_recs)
		self.back_rec_last  = None if  num_trades == 0 else self.loc_trades_recs[num_trades-1]
		#print("CTDataSet_ATrades_back::onCB_RecAdd, last:", self.back_rec_last, ", new:", obj_rec)
		return super(CTDataSet_ATrades_back, self).onCB_RecAdd(flag_plus, obj_rec, idx_rec)


class CTDataSet_ABooks_back(ktdata.CTDataSet_ABooks):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_ABooks_back, self).__init__(logger, obj_container, wreq_args)
		self.back_rec_last  = None

	def onCB_DataClean(self):
		self.back_rec_last  = None
		return super(CTDataSet_ABooks_back, self).onCB_DataClean()

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		return super(CTDataSet_ABooks_back, self).onCB_RecAdd(flag_plus, obj_rec, idx_rec)


class CTDataSet_ACandles_back(ktdata.CTDataSet_ACandles):
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ACandles_back, self).__init__(logger, recs_size, obj_container, wreq_args)
		self.back_rec_last  = None

	def onCB_DataClean(self):
		self.back_rec_last  = None
		return super(CTDataSet_ACandles_back, self).onCB_DataClean()

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		num_candles = len(self.loc_candle_recs)
		self.back_rec_last  = None if num_candles == 0 else self.loc_candle_recs[num_candles-1]
		#print("CTDataSet_ACandles_back::onCB_RecAdd, last=", self.back_rec_last)
		return super(CTDataSet_ACandles_back, self).onCB_RecAdd(flag_plus, obj_rec, idx_rec)


