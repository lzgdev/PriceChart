
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

	def onInit_DataSource_alloc(self, url_scheme, url_netloc, url):
		obj_datasrc = None
		if   url_scheme == 'https' and url_netloc == 'api.bitfinex.com':
			obj_datasrc = CTDataInput_HttpBfx_Down(self.logger, self, url)
		if obj_datasrc == None:
			obj_datasrc = super(CTDataContainer_DownOut, self).onInit_DataSource_alloc(url_scheme, url_netloc, url)
		return obj_datasrc

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


class CTDataInput_HttpBfx_Down(ktdata.CTDataInput_HttpBfx):
	def __init__(self, logger, obj_container, url_http_pref):
		ktdata.CTDataInput_HttpBfx.__init__(self, logger, obj_container, url_http_pref)
		self.loc_mark_end   = -1

	def onMark_ChanEnd_impl(self):
		self.loc_mark_end   = self.tup_run_stat[1]

	def onMts_ReqStart_impl(self):
		if self.loc_mark_end == self.tup_run_stat[1]:
			return -1
		mts_rec_last = 0
		# try to load last doc from db
		rec_last = self.obj_container.list_tups_datachan[self.tup_run_stat[2]][1].getDoc_OutLast()
		if rec_last != None:
			mts_rec_last = rec_last['mts']
		return mts_rec_last


