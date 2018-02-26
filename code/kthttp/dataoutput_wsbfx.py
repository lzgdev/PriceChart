
import ktdata

class CTDataOut_WsBfx(ktdata.CTDataOutput):
	def __init__(self, logger, obj_dataset, obj_netconn):
		ktdata.CTDataOutput.__init__(self, logger)
		self.obj_dataset    = obj_dataset
		self.obj_netconn    = obj_netconn

	# implementations for DataContainer
	def onPrep_OutChan_impl(self, **kwargs):
		#print("CTDataOut_WsBfx::onPrep_OutChan_impl", kwargs)
		out_subscribed = { 'event': 'subscribed', 'channel': kwargs['name_chan'], 'chanId': kwargs['id_chan'], }
		out_subscribed.update(kwargs['dict_args'])
		self.obj_netconn.write_message(out_subscribed)
		return True

	def onOut_DatOne_impl(self, dat_one):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_one]))
		return 1

	def onOut_DatArray_impl(self, dat_array):
		self.obj_netconn.write_message(str([self.obj_dataset.id_chan, dat_array]))
		return len(dat_array)


class CTDataOut_WsBfx_stat01(CTDataOut_WsBfx):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTDataOut_WsBfx.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec =  True

	def onTran_Doc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['mts'], doc_rec['open'], doc_rec['close'], doc_rec['high'], doc_rec['low'], doc_rec['volume'], ]
		return dat_unit


class CTDataOut_WsBfx_ticker(CTDataOut_WsBfx):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTDataOut_WsBfx.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec =  True

	def onSynAppend_get_list(self, msec_now):
		return None

	def onTran_Doc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['bid'], doc_rec['bid_size'], doc_rec['ask'], doc_rec['ask_size'],
					doc_rec['daily_change'], doc_rec['daily_change_perc'], doc_rec['last_price'],
					doc_rec['volume'], doc_rec['high'], doc_rec['low'],
				]
		return dat_unit


class CTDataOut_WsBfx_trades(CTDataOut_WsBfx):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTDataOut_WsBfx.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec =  True

	def onSynAppend_get_list(self, msec_now):
		return None

	def onTran_Doc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['tid'], doc_rec['mts'], doc_rec['amount'], doc_rec['price'], ]
		return dat_unit


class CTDataOut_WsBfx_book(CTDataOut_WsBfx):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTDataOut_WsBfx.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec =  True

	def onSynAppend_get_list(self, msec_now):
		return None

	def onTran_Doc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['price'], doc_rec['count'],
					doc_rec['amount'] if doc_rec['type'] == 'bid' else (0.0 - doc_rec['amount']), ]
		return dat_unit


class CTDataOut_WsBfx_candles(CTDataOut_WsBfx):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTDataOut_WsBfx.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec =  True

	def onSynAppend_get_list(self, msec_now):
		return None

	def onTran_Doc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['mts'], doc_rec['open'], doc_rec['close'], doc_rec['high'], doc_rec['low'], doc_rec['volume'], ]
		return dat_unit


