
import json

import ktdata
import kthttp

from .dataset_stat      import CTDataSet_Stat_Dbg, CTDataSet_Stat_Stat01,  \
							CTDataSet_Ticker_Stat, CTDataSet_ATrades_Stat, CTDataSet_ABooks_Stat, CTDataSet_ACandles_Stat

from .dataoutput_stat   import CTDataOut_Stat_stat01,  \
							CTDataOut_Stat_ticker, CTDataOut_Stat_trades, CTDataOut_Stat_book, CTDataOut_Stat_candles


class CTDataContainer_StatOut(kthttp.CTDataContainer_HttpOut):
	def __init__(self, logger, obj_outconn):
		kthttp.CTDataContainer_HttpOut.__init__(self, logger, obj_outconn)

		self.stat_tid_last  = None

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
		stat_key  = dict_args['stat'] if 'stat' in dict_args else None
		#print("CTDataContainer_StatOut::onChan_DataSet_alloc", name_chan, wreq_args, stat_key)
		obj_dataset = None
		if   name_chan == 'stat' and stat_key == 'stat01':
			obj_dataset = CTDataSet_Stat_Stat01(512, self.logger, self, wreq_args)
		elif name_chan == 'ticker':
			obj_dataset = CTDataSet_Ticker_Stat(self.logger, self, wreq_args)
		elif name_chan == 'trades':
			obj_dataset = CTDataSet_ATrades_Stat(512, self.logger, self, wreq_args)
		elif name_chan == 'book':
			obj_dataset = CTDataSet_ABooks_Stat(self.logger, self, wreq_args)
		elif name_chan == 'candles':
			obj_dataset = CTDataSet_ACandles_Stat(512, self.logger, self, wreq_args)
		else:
			obj_dataset = None
		if obj_dataset == None:
			super(CTDataContainer_StatOut, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		return obj_dataset

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		#print("CTDataContainer_StatOut::onChan_DataOut_alloc", name_chan, wreq_args)
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDataOut_Stat_ticker(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'trades':
			obj_dataout = CTDataOut_Stat_trades(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'book':
			obj_dataout = CTDataOut_Stat_book(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'candles':
			obj_dataout = CTDataOut_Stat_candles(self.logger, obj_dataset, self.obj_outconn)
		if obj_dataout == None:
			obj_dataout = super(CTDataContainer_StatOut, self).onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args, dict_args)
		return obj_dataout

	def onExec_Prep_impl(self, arg_prep):
		#print("CTDataContainer_StatOut::onExec_Prep_impl(01)", self.obj_dset_trades, self.obj_dset_candles)
		self.obj_dset_trades  = None
		self.obj_dset_candles = None
		num_chans = len(self.list_tups_datachan)
		for idx_chan in range(num_chans):
			if   isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ATrades):
				self.obj_dset_trades  = self.list_tups_datachan[idx_chan][0]
			elif isinstance(self.list_tups_datachan[idx_chan][0], ktdata.CTDataSet_ACandles):
				self.obj_dset_candles = self.list_tups_datachan[idx_chan][0]
		#print("CTDataContainer_StatOut::onExec_Prep_impl(11)", self.obj_dset_trades, self.obj_dset_candles)
		self.stamp_exec_bgn = self.mtsNow_time()

	def onExec_Post_impl(self, arg_post):
		self.stamp_exec_end = self.mtsNow_time()
		print("CTDataContainer_StatOut::onExec_Post_impl, cost:", format(self.stamp_exec_end-self.stamp_exec_bgn, ","))
		if self.obj_dset_trades  != None:
			for rec_trade in self.obj_dset_trades.loc_trades_recs:
				print("CTDataContainer_StatOut::onExec_Post_impl, trade:", rec_trade)
				self.stat_tid_last  = rec_trade['tid']
		if self.obj_dset_candles != None:
			for rec_candle in self.obj_dset_candles.loc_candle_recs:
				print("CTDataContainer_StatOut::onExec_Post_impl, candle:", rec_candle)


