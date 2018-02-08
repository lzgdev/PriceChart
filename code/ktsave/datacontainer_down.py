
import json
import urllib.parse

import ktdata

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


class CTDataContainer_DownOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_dbwriter  = None

	def onExec_Init_ext(self, list_exec):
		str_db_uri  = 'mongodb://127.0.0.1:27017'
		str_db_name = 'bfx-pub'
		self.obj_dbwriter  = ktdata.KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
		return None

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDataOut_Db_ticker(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'trades':
			obj_dataout = CTDataOut_Db_trades(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'book':
			obj_dataout = CTDataOut_Db_book(self.logger, obj_dataset, self.obj_dbwriter)
		elif name_chan == 'candles':
			obj_dataout = CTDataOut_Db_candles(self.logger, obj_dataset, self.obj_dbwriter)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		#print("CTDataContainer_DownOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		if obj_dataout != None:
			obj_dataout.prepOutChan(name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=obj_dataset.wreq_args)

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass

	@staticmethod
	def isNextRunAfter(url, jobs):
		flag_run_after = False
		url_parse = urllib.parse.urlparse(url)
		if   url_parse.scheme == 'https' and url_parse.netloc == 'api.bitfinex.com':
			flag_run_after =  True
		#flag_run_after = False
		return  flag_run_after

