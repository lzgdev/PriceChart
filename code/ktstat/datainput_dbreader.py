
import ktdata

class CTDataInput_DbReader(ktdata.CTDataInput_Db):
	def __init__(self, logger, obj_container):
		ktdata.CTDataInput_Db.__init__(self, logger, obj_container)
		self.loc_load_args  = None

		self.flag_run_num   = 0

	def onPrep_Read_impl(self, **kwargs):
		#print("CTDataInput_DbReader::onPrep_Read_impl, args:", dict(kwargs))
		if self.obj_dbadapter == None:
			self.obj_dbadapter = ktdata.KTDataMedia_DbReader(self.logger, self)
			self.obj_dbadapter.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
		self.loc_name_chan  = kwargs['name_chan']
		self.loc_wreq_args  = kwargs['wreq_args']
		self.loc_load_args  = kwargs.get('load_args', None)

		self.flag_run_num   = 1
		return True

	def onLoop_ReadPrep_impl(self):
		if self.id_data_chan == None:
			self.id_data_chan = self.obj_container.datIN_ChanGet(self.loc_name_chan, self.loc_wreq_args)
		if self.id_data_chan == None:
			ktdata.CTDataInput_Db.gid_chan_now += 1
			self.id_data_chan  = ktdata.CTDataInput_Db.gid_chan_now
			self.obj_container.datIN_ChanAdd(self.id_data_chan, self.loc_name_chan, self.loc_wreq_args)
		#print("CTDataInput_DbReader::onLoop_ReadPrep_impl, id_chan:", self.id_data_chan, ", num:", self.flag_run_num, self.loc_name_chan, self.loc_wreq_args)
		if self.flag_run_num <  1:
			return False
		return True

	def onLoop_ReadMain_impl(self):
		name_dbtbl = ktdata.CTDataContainer._gmap_TaskChans_dbtbl(self.loc_name_chan, self.loc_wreq_args)
		args_load = self.loc_load_args if self.loc_load_args != None else { }
		#print("CTDataInput_DbReader::onLoop_ReadMain_impl, name_dbtbl:", name_dbtbl, ", args_load", args_load)
		self.obj_dbadapter.dbOP_CollLoad(self.id_data_chan, name_dbtbl, **args_load)

	def onLoop_ReadPost_impl(self):
		self.flag_run_num -= 1
		#if self.id_data_chan != None:
		#	self.obj_container.datIN_ChanDel(self.id_data_chan)
		#self.id_data_chan = None


