
import ktdata

class CTDataInput_DbReader(ktdata.CTDataInput_Db):
	def __init__(self, logger, obj_container):
		ktdata.CTDataInput_Db.__init__(self, logger, obj_container)

	def onPrep_Read_impl(self, **kwargs):
		#print("CTDataInput_DbReader::onPrep_Read_impl, args:", dict(kwargs))
		if self.obj_dbadapter == None:
			self.obj_dbadapter = ktdata.KTDataMedia_DbReader(self.logger, self)
			self.obj_dbadapter.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		self.loc_name_chan  = kwargs['name_chan']
		self.loc_wreq_args  = kwargs['wreq_args']
		return True

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
		self.name_dbtbl = ktdata.CTDataContainer._gmap_TaskChans_dbtbl(self.loc_name_chan, self.loc_wreq_args)
		find_args = {}
		#find_args = { 'mts': { '$gt': 1505413152000, '$lt': 1505413452000, } }
		#find_args = { 'mts': { '$eq': 1505413376000, } }
		#find_args = { 'mts': { '$gte': 1505413380000, '$lt': 1505413440000, } }
		#find_args = { 'mts': { '$gte': 1505413440000, '$lt': 1505413500000, } }
		sort_args = None
		self.obj_dbadapter.dbOP_CollLoad(self.id_data_chan, self.name_dbtbl, find_args, sort_args)


