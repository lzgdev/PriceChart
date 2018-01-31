
import ktdata

class CTDataSet_Ticker_Stat(ktdata.CTDataSet_Ticker):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_Ticker_Stat, self).__init__(logger, obj_container, wreq_args)

class CTDataSet_ATrades_Stat(ktdata.CTDataSet_ATrades):
	def __init__(self, recs_size, logger, obj_container, wreq_args):
		super(CTDataSet_ATrades_Stat, self).__init__(recs_size, logger, obj_container, wreq_args)
		"""
		self.sum_amount = 0.0
		self.rec_min = 99999.9
		self.rec_max = 0.0
		"""

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		"""
		rec_amount = obj_rec['amount']
		if self.rec_min > obj_rec['price']:
			self.rec_min = obj_rec['price']
		if self.rec_max < obj_rec['price']:
			self.rec_max = obj_rec['price']
		self.sum_amount += rec_amount if rec_amount >= 0 else (0.0 - rec_amount)
		"""
		#print("CTDataSet_ATrades_Stat::onCB_RecAdd", self.rec_min, self.rec_max, self.sum_amount, flag_plus, idx_rec, obj_rec)
		pass

class CTDataSet_ABooks_Stat(ktdata.CTDataSet_ABooks):
	def __init__(self, logger, obj_container, wreq_args):
		super(CTDataSet_ABooks_Stat, self).__init__(logger, obj_container, wreq_args)

class CTDataSet_ACandles_Stat(ktdata.CTDataSet_ACandles):
	def __init__(self, recs_size, logger, obj_container, wreq_args):
		super(CTDataSet_ACandles_Stat, self).__init__(recs_size, logger, obj_container, wreq_args)

	def onCB_DataClean(self):
		pass

	def onCB_DataSync(self):
		pass

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		#print("CTDataSet_ACandles_Stat::onCB_RecAdd", flag_plus, idx_rec, obj_rec)
		pass


