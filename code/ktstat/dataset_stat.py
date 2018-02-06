
import sys
import math

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
		self.num_msec_stat = 60 * 1000
		self.loc_idx_stat  = None
		self.privStat_clean()

	def onCB_RecAdd(self, flag_plus, obj_rec, idx_rec):
		mts_doc  = obj_rec['mts']
		idx_stat = math.floor(mts_doc / self.num_msec_stat)
		if idx_stat != self.loc_idx_stat:
			if self.dbg_stat >= 3 and self.loc_idx_stat != None:
				mts_stat = (idx_stat - 1) * self.num_msec_stat
				print("CTDataSet_ATrades_Stat::onCB_RecAdd", format(mts_stat, ","), ", min:", self.rec_min, ", max:", self.rec_max, ", vol:", self.sum_amount)
			self.privStat_clean()
			self.loc_idx_stat = idx_stat
		if self.dbg_stat >= 2:
			print("CTDataSet_ATrades_Stat::onCB_RecAdd", format(mts_doc, ","), ", doc:", obj_rec)

		if self.rec_min > obj_rec['price']:
			self.rec_min = obj_rec['price']
		if self.rec_max < obj_rec['price']:
			self.rec_max = obj_rec['price']
		self.sum_amount += abs(obj_rec['amount'])


	def privStat_clean(self):
		self.sum_amount = 0.0
		self.rec_min = sys.maxsize
		self.rec_max = 0.0


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


