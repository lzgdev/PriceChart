
import json
import urllib.parse

import ktdata

from .datainput_dbreader    import CTDataInput_DbReader

from .dataset_stat      import CTDataSet_Ticker_Stat, CTDataSet_ATrades_Stat, CTDataSet_ABooks_Stat, CTDataSet_ACandles_Stat

from .dataoutput_stat   import CTDataOut_Stat_ticker, CTDataOut_Stat_trades, CTDataOut_Stat_book, CTDataOut_Stat_candles
from .dataoutput_wsbfx  import CTDataOut_WsBfx_ticker, CTDataOut_WsBfx_trades, CTDataOut_WsBfx_book, CTDataOut_WsBfx_candles


class CTDataContainer_StatOut(ktdata.CTDataContainer):
	def __init__(self, logger, obj_outconn):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_outconn = obj_outconn
		self.flag_out_wsbfx = False

	def onExec_Init_impl(self, **kwargs):
		name_chan = kwargs['name_chan']
		wreq_args = kwargs['wreq_args']
		#name_chan = 'candles'
		#wreq_args = '{ "key": "trade:1m:tBTCUSD" }'
		#name_chan = 'trades'
		#wreq_args = '{ "symbol": "tBTCUSD" }'

		idx_data_chan = self.addArg_DataChannel(name_chan, wreq_args, None)
		if idx_data_chan >= 0:
			self.addObj_DataSource(CTDataInput_DbReader(self.logger, self),
						name_chan=name_chan, wreq_args=wreq_args)

		return None

	def onExec_Prep_impl(self):
		pass

	def onChan_DataSet_alloc(self, name_chan, wreq_args):
		obj_dataset = None
		if   name_chan == 'ticker':
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
			super(CTDataContainer_StatOut, self).onChan_DataSet_alloc(name_chan, wreq_args)
		return obj_dataset

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args):
		obj_dataout = None
		if self.flag_out_wsbfx:
			if   name_chan == 'ticker':
				obj_dataout = CTDataOut_WsBfx_ticker(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'trades':
				obj_dataout = CTDataOut_WsBfx_trades(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'book':
				obj_dataout = CTDataOut_WsBfx_book(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'candles':
				obj_dataout = CTDataOut_WsBfx_candles(self.logger, obj_dataset, self.obj_outconn)
		else:
			if   name_chan == 'ticker':
				obj_dataout = CTDataOut_Stat_ticker(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'trades':
				obj_dataout = CTDataOut_Stat_trades(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'book':
				obj_dataout = CTDataOut_Stat_book(self.logger, obj_dataset, self.obj_outconn)
			elif name_chan == 'candles':
				obj_dataout = CTDataOut_Stat_candles(self.logger, obj_dataset, self.obj_outconn)
		if obj_dataout == None:
			obj_dataout = super(CTDataContainer_StatOut, self).onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		#print("CTDataContainer_StatOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		if obj_dataout != None:
			obj_dataout.prepOutChan(id_chan=id_chan, name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=tup_chan[4])

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

