
import json
import urllib.parse

import time

import ktdata

from .dataoutput_db     import CTDataOut_Db_ticker, CTDataOut_Db_trades, CTDataOut_Db_book, CTDataOut_Db_candles


class CTDataContainer_DownOut(ktdata.CTDataContainer):
	def __init__(self, logger):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_dbwriter  = None

	def onExec_Init_ext(self, list_exec):
		str_db_uri  = 'mongodb://127.0.0.1:27017'
		str_db_name = 'bfx-down'
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

	def onChan_DataSet_alloc(self, name_chan, wreq_args, dict_args):
		obj_dataset = None
		if   name_chan ==  'trades':
			obj_dataset = CTDataSet_ATrades_down(self.logger, self.size_dset_trades, self, wreq_args)
		elif name_chan == 'candles':
			obj_dataset = CTDataSet_ACandles_down(self.logger, self.size_dset_candles, self, wreq_args)
		if obj_dataset == None:
			obj_dataset = super(CTDataContainer_DownOut, self).onChan_DataSet_alloc(name_chan, wreq_args, dict_args)
		return obj_dataset

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

#	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
#		pass


class CTDataInput_HttpBfx_Down(ktdata.CTDataInput_HttpBfx):
	def __init__(self, logger, obj_container, url_http_pref):
		ktdata.CTDataInput_HttpBfx.__init__(self, logger, obj_container, url_http_pref)
		self.loc_mark_end   = -1

		#self.flag_dbg_in  = 1

	def onMark_ChanEnd_impl(self):
		self.loc_mark_end   = self.tup_run_stat[1]

	def onMts_ReqRange_impl(self):
		if self.loc_mark_end == self.tup_run_stat[1]:
			return None
		# WARNING: call ktdata.CTDataContainer.__chan_Dwreq2Idx(...) here
		idx_chan = self.obj_container._CTDataContainer__chan_Dwreq2Idx(self.tup_run_stat[3], self.tup_run_stat[4])
		if idx_chan <  0:
			return None
		rec_last = None
		if self.obj_container.list_tups_datachan[idx_chan][1] != None:
			rec_last = self.obj_container.list_tups_datachan[idx_chan][1].getDoc_OutLast()
		mts_last = None if rec_last == None else rec_last.get('mts', None)
		if mts_last != None and self.flag_dbg_in >= 1:
			self.logger.info(self.inf_this + " onMts_ReqRange_impl, chan=" + str(self.tup_run_stat[3]) +
								", New End=" + str(mts_last) + " " +
								time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(mts_last/1000))))
		return None if mts_last == None else (mts_last, -1)


class CTDataSet_ACandles_down(ktdata.CTDataSet_ACandles):
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ACandles_down, self).__init__(logger, recs_size, obj_container, wreq_args)

	def onLocDataSync_impl(self):
		super(CTDataSet_ACandles_down, self).onLocDataSync_impl()
		if len(self.loc_candle_recs) >  0:
			rec_ign = self.loc_candle_recs.pop()
			if self.flag_dbg_set >= 1:
				self.logger.warning(self.inf_this + " onLocDataSync_impl, rec_ign=" + str(rec_ign))


class CTDataSet_ATrades_down(ktdata.CTDataSet_ATrades):
	def __init__(self, logger, recs_size, obj_container, wreq_args):
		super(CTDataSet_ATrades_down, self).__init__(logger, recs_size, obj_container, wreq_args)

	def onLocDataSync_impl(self):
		super(CTDataSet_ATrades_down, self).onLocDataSync_impl()
		idx_sep  = None
		mts_sep  = None
		for idx_rec in range(len(self.loc_trades_recs)-1, -1, -1):
			mts_this = self.loc_trades_recs[idx_rec].get('mts', 0)
			if mts_sep != None and mts_this <  mts_sep:
				break
			idx_sep  = idx_rec
			mts_sep  = mts_this
		if idx_sep != None and idx_sep >  0:
			while len(self.loc_trades_recs) >  idx_sep:
				rec_ign = self.loc_trades_recs.pop()
				if self.flag_dbg_set >= 1:
					self.logger.warning(self.inf_this + " onLocDataSync_impl, rec_ign=" + str(rec_ign))


