
from .datacontainer_net         import CTDataContainer_NetOut

class CTDataContainer_WsBfxOut(CTDataContainer_NetOut):
	def __init__(self, logger, obj_netconn):
		CTDataContainer_NetOut.__init__(self, logger, obj_netconn)

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args):
		obj_dataout = None
		if    'ticker' == name_chan:
			obj_dataout = CTNetOut_Adapter_ticker_WsBfx(self.logger, obj_dataset, self.obj_netconn)
		elif  'trades' == name_chan:
			obj_dataout = CTNetOut_Adapter_trades_WsBfx(self.logger, obj_dataset, self.obj_netconn)
		elif    'book' == name_chan:
			obj_dataout = CTNetOut_Adapter_book_WsBfx(self.logger, obj_dataset, self.obj_netconn)
		elif 'candles' == name_chan:
			obj_dataout = CTNetOut_Adapter_candles_WsBfx(self.logger, obj_dataset, self.obj_netconn)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		tup_chan  = self.list_tups_datachan[idx_chan]
		#print("CTDataContainer_WsBfxOut::onDatIN_ChanAdd_ext, id_chan:", id_chan, ", tup_chan:", tup_chan)
		out_subscribed = { 'event': 'subscribed', 'channel': tup_chan[2], 'chanId': id_chan, }
		out_subscribed.update(tup_chan[4])
		self.obj_netconn.write_message(out_subscribed)
		"""
		if obj_dataout != None:
			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)
		"""

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass


from .datacontainer_net import CTNetOut_Adapter_ticker, CTNetOut_Adapter_trades, CTNetOut_Adapter_book, CTNetOut_Adapter_candles

class CTNetOut_Adapter_ticker_WsBfx(CTNetOut_Adapter_ticker):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter_ticker.__init__(self, logger, obj_dataset, obj_netconn)

	def onSend_DatPlus_impl(self, dat_unit):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_unit]))

	def onSend_DatArray_impl(self, dat_array):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_array]))

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['bid'], doc_rec['bid_size'], doc_rec['ask'], doc_rec['ask_size'],
					doc_rec['daily_change'], doc_rec['daily_change_perc'], doc_rec['last_price'],
					doc_rec['volume'], doc_rec['high'], doc_rec['low'],
				]
		return dat_unit


class CTNetOut_Adapter_trades_WsBfx(CTNetOut_Adapter_trades):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter_trades.__init__(self, logger, obj_dataset, obj_netconn)

	def onSend_DatPlus_impl(self, dat_unit):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_unit]))

	def onSend_DatArray_impl(self, dat_array):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_array]))

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['tid'], doc_rec['mts'], doc_rec['amount'], doc_rec['price'], ]
		return dat_unit


class CTNetOut_Adapter_book_WsBfx(CTNetOut_Adapter_book):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter_book.__init__(self, logger, obj_dataset, obj_netconn)

	def onSend_DatPlus_impl(self, dat_unit):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_unit]))

	def onSend_DatArray_impl(self, dat_array):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_array]))

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['price'], doc_rec['count'],
					doc_rec['amount'] if doc_rec['type'] == 'bid' else (0.0 - doc_rec['amount']), ]
		return dat_unit


class CTNetOut_Adapter_candles_WsBfx(CTNetOut_Adapter_candles):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter_candles.__init__(self, logger, obj_dataset, obj_netconn)

		#self.flag_dbg_rec =  True

	def onSend_DatPlus_impl(self, dat_unit):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_unit]))

	def onSend_DatArray_impl(self, dat_array):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_array]))

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['mts'], doc_rec['open'], doc_rec['close'], doc_rec['high'], doc_rec['low'], doc_rec['volume'], ]
		return dat_unit


