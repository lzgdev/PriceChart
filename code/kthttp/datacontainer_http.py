
import json

import ktdata

from .dataoutput_wsbfx      import CTDataOut_WsBfx_stat01, \
							CTDataOut_WsBfx_ticker, CTDataOut_WsBfx_trades, CTDataOut_WsBfx_book, CTDataOut_WsBfx_candles


class CTDataContainer_HttpOut(ktdata.CTDataContainer):
	def __init__(self, logger, obj_outconn):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_outconn = obj_outconn
		self.flag_out_wsbfx = False

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		#print("CTDataContainer_HttpOut::onChan_DataOut_alloc", name_chan, wreq_args)
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
		if obj_dataout == None:
			obj_dataout = super(CTDataContainer_HttpOut, self).onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args, dict_args)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		#print("CTDataContainer_HttpOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		if obj_dataout != None:
			obj_dataout.prepOutChan(id_chan=id_chan, name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, dict_args=tup_chan[4])

#	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
#		pass


