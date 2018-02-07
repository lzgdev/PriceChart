
import ktdata

class CTDataSet_Stat_Dbg:
	def __init__(self):
		self.dbg_stat = 0


class CTDataSet_Stat_Stat01(ktdata.CTDataSet_AStat, CTDataSet_Stat_Dbg):
	def __init__(self, recs_size, logger, obj_container, wreq_args):
		ktdata.CTDataSet_AStat.__init__(self, recs_size, logger, obj_container, wreq_args)
		CTDataSet_Stat_Dbg.__init__(self)

	def onCB_DataClean(self):
		pass

	def onCB_DataSync(self):
		pass

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		mts_doc  = obj_rec['mts']
		if self.dbg_stat >= 2:
			print("CTDataSet_Stat_Stat01::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)


class CTDataSet_Ticker_Stat(ktdata.CTDataSet_Ticker, CTDataSet_Stat_Dbg):
	def __init__(self, logger, obj_container, wreq_args):
		ktdata.CTDataSet_Ticker.__init__(self, logger, obj_container, wreq_args)
		CTDataSet_Stat_Dbg.__init__(self)

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		mts_doc  = obj_rec['mts']
		if self.dbg_stat >= 2:
			print("CTDataSet_Ticker_Stat::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)


class CTDataSet_ATrades_Stat(ktdata.CTDataSet_ATrades, CTDataSet_Stat_Dbg):
	def __init__(self, recs_size, logger, obj_container, wreq_args):
		ktdata.CTDataSet_ATrades.__init__(self, recs_size, logger, obj_container, wreq_args)
		CTDataSet_Stat_Dbg.__init__(self)

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		mts_doc  = obj_rec['mts']
		if self.dbg_stat >= 2:
			print("CTDataSet_ATrades_Stat::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)


class CTDataSet_ABooks_Stat(ktdata.CTDataSet_ABooks, CTDataSet_Stat_Dbg):
	def __init__(self, logger, obj_container, wreq_args):
		ktdata.CTDataSet_ABooks.__init__(self, logger, obj_container, wreq_args)
		CTDataSet_Stat_Dbg.__init__(self)

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		if self.dbg_stat >= 2:
			mts_doc = obj_rec['mts']
			print("CTDataSet_ABooks_Stat::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)


class CTDataSet_ACandles_Stat(ktdata.CTDataSet_ACandles, CTDataSet_Stat_Dbg):
	def __init__(self, recs_size, logger, obj_container, wreq_args):
		ktdata.CTDataSet_ACandles.__init__(self, recs_size, logger, obj_container, wreq_args)
		CTDataSet_Stat_Dbg.__init__(self)

	def onCB_DataClean(self):
		pass

	def onCB_DataSync(self):
		pass

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		if self.dbg_stat >= 2:
			mts_doc = obj_rec['mts']
			print("CTDataSet_ACandles_Stat::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)
		pass


