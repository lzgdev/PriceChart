
import ktdata

class CTDataContainer_NetOut(ktdata.CTDataContainer):
	def __init__(self, logger, obj_netconn):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_netconn = obj_netconn

	def onExec_Init_impl(self, **kwargs):
		name_chan = kwargs['name_chan']
		wreq_args = kwargs['wreq_args']

		idx_data_chan = self.addArg_DataChannel(name_chan, wreq_args, None)
		if idx_data_chan >= 0:
			self.addObj_DataSource(CTDataInput_Db_NetReader(self.logger, self),
						name_chan=name_chan, wreq_args=wreq_args)

		return None

	def onExec_Prep_impl(self):
		pass

	def onDatCB_DataClean_impl(self, idx_chan, obj_dataset):
		pass

	def onDatCB_DataSync_impl(self, idx_chan, obj_dataset, msec_now):
		#print("CTDataContainer_NetOut::onDatCB_DataSync_impl", idx_chan)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.synAppend(msec_now)

	def onDatCB_RecPlus_impl(self, idx_chan, obj_dataset, doc_rec, idx_rec):
		#print("CTDataContainer_NetOut::onDatCB_RecPlus_impl", idx_chan, doc_rec, idx_rec)
		obj_dataout = self.list_tups_datachan[idx_chan][1]
		if obj_dataout != None:
			obj_dataout.docAppend(doc_rec)


import time
import math
import copy

class CTDataInput_Db_NetReader(ktdata.CTDataInput_Db):
	def __init__(self, logger, obj_container):
		ktdata.CTDataInput_Db.__init__(self, logger, obj_container)

	def onPrep_Read_impl(self, **kwargs):
		#print("CTDataInput_Db_NetReader::onPrep_Read_impl, args:", dict(kwargs))
		if self.obj_dbadapter == None:
			self.obj_dbadapter = ktdata.KTDataMedia_DbReader(self.logger, self)
			self.obj_dbadapter.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		self.loc_name_chan  = kwargs['name_chan']
		self.loc_wreq_args  = kwargs['wreq_args']
		return True

	def onInit_DbPrep_impl(self):
		#print("CTDataInput_Db_NetReader::onPrep_Read_impl ...", self.flag_run_num, self.loc_name_chan, self.loc_wreq_args)
		if self.id_data_chan == None:
			self.gid_chan_now += 1
			self.id_data_chan  = self.gid_chan_now
			self.obj_container.datIN_ChanAdd(self.id_data_chan, self.loc_name_chan, self.loc_wreq_args)
		self.flag_run_num -= 1
		if self.flag_run_num <  0:
			return False
		return True

	def onExec_DbRead_impl(self):
		#print("CTDataInput_Db_NetReader::onExec_DbRead_impl ...")
		#self.name_dbtbl = 'candles-tBTCUSD-1m'
		self.name_dbtbl = ktdata.CTDataContainer._gmap_TaskChans_dbtbl(self.loc_name_chan, self.loc_wreq_args)
		find_args = {}
		sort_args = None
		self.obj_dbadapter.dbOP_CollLoad(self.id_data_chan, self.name_dbtbl, find_args, sort_args)
		"""
		self.obj_db_reader = None
		self.obj_db_reader = KTDataMedia_DbReader_WsOut(self.logger)
		self.obj_db_reader.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		obj_doc   = self.obj_db_reader.dbOP_DocFind_One(ktdata.COLLNAME_CollSet, filt_args, [('$nature', -1)])
		self.obj_db_reader.setDataset(obj_dataset)
		self.obj_db_reader.dbOP_CollLoad(id_chan_sbsc, name_coll, {}, None)
		"""


class CTNetOut_Adapter(ktdata.CTDataOutput):
	def __init__(self, logger, obj_dataset, obj_netconn):
		ktdata.CTDataOutput.__init__(self, logger)
		self.obj_dataset    = obj_dataset
		self.obj_netconn    = obj_netconn
		self.flag_dbg_rec   = False

	def sendDatPlus(self, dat_unit):
		return self.onSend_DatPlus_impl(dat_unit)

	def sendDatArray(self, dat_array):
		return self.onSend_DatArray_impl(dat_array)

	def tranDoc2Dat(self, doc_rec):
		return self.onTranDoc2Dat_impl(doc_rec)

	def onDocAppend_impl(self, doc_rec):
		#print("CTNetOut_Adapter::onDocAppend_impl", doc_rec)
		dat_unit = self.tranDoc2Dat(doc_rec)
		dat_sent = None
		self.sendDatPlus(dat_unit)
		if self.flag_dbg_rec:
			self.logger.info("CTNetOut_Adapter(onSynAppend_impl): dat_unit=" + str(dat_unit) + ", doc_rec=" + str(dat_sent))
		return dat_unit

	def onSend_DatPlus_impl(self, dat_unit):
		pass

	def onSend_DatArray_impl(self, dat_array):
		pass

	def onTranDoc2Dat_impl(self, doc_rec):
		return None

	def onSynAppend_impl(self, msec_now):
		return None

class CTNetOut_Adapter_ticker(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, obj_netconn)


class CTNetOut_Adapter_trades(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, obj_netconn)

	def onSynAppend_impl(self, msec_now):
		dat_array = []
		for trade_rec in self.obj_dataset.loc_trades_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTNetOut_Adapter_trades(onSynAppend_impl): syn_rec=" + str(trade_rec))
			dat_array.append(self.tranDoc2Dat(trade_rec))
		self.sendDatArray(dat_array)


class CTNetOut_Adapter_book(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, obj_netconn)

	def onSynAppend_impl(self, msec_now):
		# add docs from snapshot
		dat_array = []
		for book_rec in self.obj_dataset.loc_book_bids:
			dat_array.append(self.tranDoc2Dat(book_rec))
		for book_rec in self.obj_dataset.loc_book_asks:
			dat_array.append(self.tranDoc2Dat(book_rec))
		self.sendDatArray(dat_array)


class CTNetOut_Adapter_candles(CTNetOut_Adapter):
	def __init__(self, logger, obj_dataset, obj_netconn):
		CTNetOut_Adapter.__init__(self, logger, obj_dataset, obj_netconn)
		#self.flag_dbg_rec   = True

	def onSynAppend_impl(self, msec_now):
		dat_array = []
		for candle_rec in self.obj_dataset.loc_candle_recs:
			if self.flag_dbg_rec:
				self.logger.info("CTNetOut_Adapter_candles(onSynAppend_impl): syn_rec=" + str(candle_rec))
			dat_array.append(self.tranDoc2Dat(candle_rec))
		self.sendDatArray(dat_array)


