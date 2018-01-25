
import ktdata

class CTDataContainer_NetOut(ktdata.CTDataContainer):
	def __init__(self, logger, obj_netconn):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_netconn = obj_netconn

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

class CTDataInput_DbReader(ktdata.CTDataInput_Db):
	gid_chan_now = 11

	def __init__(self, logger, obj_container):
		ktdata.CTDataInput_Db.__init__(self, logger, obj_container)
		self.obj_dbadapter  = None
		self.flag_rec_plus  = True
		self.list_tmp_docs  = []

		self.id_data_chan   = None
		self.loc_name_chan  = None
		self.loc_wreq_args  = None

		self.flag_run_num   = 1

	def onPrep_Read_impl(self, **kwargs):
		#print("CTDataInput_DbReader::onPrep_Read_impl, args:", dict(kwargs))
		if self.obj_dbadapter == None:
			self.obj_dbadapter = KTDataMedia_DbReader_Adp(self.logger, self)
			self.obj_dbadapter.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		self.loc_name_chan  = kwargs['name_chan']
		self.loc_wreq_args  = kwargs['wreq_args']
		return True

	def datFwd_Begin(self, id_chan):
		self.onDat_Begin_impl(id_chan)

	def datFwd_Doc(self, id_chan, obj_doc):
		self.onDat_FwdDoc_impl(id_chan, obj_doc)

	def datFwd_End(self, id_chan, num_docs):
		self.onDat_End_impl(id_chan, num_docs)

	def onInit_DbPrep_impl(self):
		#print("CTDataInput_DbReader::onPrep_Read_impl ...", self.flag_run_num, self.loc_name_chan, self.loc_wreq_args)
		if self.id_data_chan == None:
			self.gid_chan_now += 1
			self.id_data_chan  = self.gid_chan_now
			self.obj_container.datIN_ChanAdd(self.id_data_chan, self.loc_name_chan, self.loc_wreq_args)
		self.flag_run_num -= 1
		if self.flag_run_num <  0:
			return False
		return True

	def onExec_DbRead_impl(self):
		#print("CTDataInput_DbReader::onExec_DbRead_impl ...")
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

	def onDat_Begin_impl(self, id_chan):
		#print("CTDataInput_DbReader::onDat_Begin_impl", id_chan)
		self.flag_rec_plus  =  True
		self.list_tmp_docs.clear()

	def onDat_End_impl(self, id_chan, num_docs):
		#print("CTDataInput_DbReader::onDat_End_impl", id_chan)
		self.flag_rec_plus  =  True
		self.list_tmp_docs.clear()

	def onDat_FwdDoc_impl(self, id_chan, obj_doc):
		type_rec = None if not 'type' in obj_doc else obj_doc['type']
		#print("CTDataInput_DbReader::onDat_FwdDoc_impl", id_chan, obj_doc)
		if   'reset' == type_rec:
			self.flag_rec_plus  = False
			self.list_tmp_docs.clear()
		elif  'sync' == type_rec:
			self.obj_container.datIN_DataFwd(id_chan, ktdata.DFMT_KKAIPRIV, self.list_tmp_docs)
			self.flag_rec_plus  =  True
			self.list_tmp_docs.clear()
		else:
			if not self.flag_rec_plus:
				#print("CTDataInput_DbReader::onDat_FwdDoc_impl b=31", id_chan, obj_doc)
				self.list_tmp_docs.append(obj_doc)
			else:
				#print("CTDataInput_DbReader::onDat_FwdDoc_impl b=32", id_chan, obj_doc)
				self.obj_container.datIN_DataFwd(id_chan, ktdata.DFMT_KKAIPRIV, obj_doc)


class KTDataMedia_DbReader_Adp(ktdata.KTDataMedia_DbReader):
	def __init__(self, logger, obj_dbreader):
		ktdata.KTDataMedia_DbReader.__init__(self, logger)
		self.obj_dbreader = obj_dbreader

	def onDbEV_CollLoad_Begin(self, id_chan):
		#self.logger.info("DataMedia(load): begin, chan=" + str(id_chan))
		self.obj_dbreader.datFwd_Begin(id_chan)

	def onDbEV_CollLoad_Doc(self, id_chan, obj_doc):
		#self.logger.info("DataMedia(load): chan=" + str(id_chan) + ", doc=" + str(obj_doc))
		self.obj_dbreader.datFwd_Doc(id_chan, obj_doc)

	def onDbEV_CollLoad_End(self, id_chan, num_docs):
		#self.logger.info("DataMedia(load): end, chan=" + str(id_chan) + ", num=" + str(num_docs))
		self.obj_dbreader.datFwd_End(id_chan, num_docs)


import ktdata

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

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['bid'], doc_rec['bid_size'], doc_rec['ask'], doc_rec['ask_size'],
					doc_rec['daily_change'], doc_rec['daily_change_perc'], doc_rec['last_price'],
					doc_rec['volume'], doc_rec['high'], doc_rec['low'], doc_rec[''],
				]
		return dat_unit

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

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['tid'], doc_rec['mts'], doc_rec['amount'], doc_rec['price'], ]
		return dat_unit


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

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['price'], doc_rec['count'],
					doc_rec['amount'] if doc_rec['type'] == 'bid' else (0.0 - doc_rec['amount']), ]
		return dat_unit


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

	def onTranDoc2Dat_impl(self, doc_rec):
		dat_unit = [ doc_rec['mts'], doc_rec['open'], doc_rec['close'], doc_rec['high'], doc_rec['low'], doc_rec['volume'], ]
		return dat_unit


