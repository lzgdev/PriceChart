
import urllib.parse

from .datainput     import CTDataInput_Db
from .datamedia_db  import KTDataMedia_DbReader

class CTDataInput_DbReader(CTDataInput_Db):
	def __init__(self, logger, obj_container, url_dbsrc):
		CTDataInput_Db.__init__(self, logger, obj_container)
		self.url_dbsrc = url_dbsrc
		self.num_chan_cfg   = -1
		self.run_chan_cfg   = 0

		self.flag_run_num   = 0

	def onPrep_Read_impl(self, **kwargs):
		#print("CTDataInput_DbReader::onPrep_Read_impl, args:", dict(kwargs))
		#self.list_chan_cfg = None
		self.num_chan_cfg = len(self.list_chan_cfg) if isinstance(self.list_chan_cfg, list) else -1
		self.run_chan_cfg = 0

		if self.obj_dbadapter == None:
			url_parse  = urllib.parse.urlparse(self.url_dbsrc)
			url_path   = url_parse.path if not url_parse.path.startswith('/') else url_parse.path[1:]
			self.obj_dbadapter = KTDataMedia_DbReader(self.logger, self)
			self.obj_dbadapter.dbOP_Connect(url_parse.scheme + '://' + url_parse.netloc, url_path)

		self.flag_run_num   = 1
		return True

	def onLoop_ReadPrep_impl(self):
		if self.run_chan_cfg >= self.num_chan_cfg:
			return False
		if self.flag_run_num <  1:
			return False
		cfg_chan = self.list_chan_cfg[self.run_chan_cfg]

		self.loc_name_chan  = cfg_chan['channel']
		self.loc_wreq_args  = cfg_chan['wreq_args']
		self.loc_load_args  = cfg_chan.get('load_args', None)

		if self.id_data_chan == None:
			tup_chan = self.obj_container.datIN_ChanGet(self.loc_name_chan, self.loc_wreq_args)
			self.id_data_chan = None if tup_chan == None else tup_chan[0]
		if self.id_data_chan == None:
			CTDataInput_Db.gid_chan_now += 1
			self.id_data_chan  = CTDataInput_Db.gid_chan_now
			self.obj_container.datIN_ChanAdd(self.id_data_chan, self.loc_name_chan, self.loc_wreq_args)
		#print("CTDataInput_DbReader::onLoop_ReadPrep_impl, id_chan:", self.id_data_chan, ", num:", self.flag_run_num, self.loc_name_chan, self.loc_wreq_args)
		return True

	def onLoop_ReadMain_impl(self):
		name_dbtbl = self.obj_container._gmap_TaskChans_dbtbl(self.loc_name_chan, self.loc_wreq_args)
		args_load = self.loc_load_args if self.loc_load_args != None else { }
		#print("CTDataInput_DbReader::onLoop_ReadMain_impl, name_dbtbl:", name_dbtbl, ", args_load", args_load)
		self.obj_dbadapter.dbOP_CollLoad(self.id_data_chan, name_dbtbl, **args_load)

	def onLoop_ReadPost_impl(self):
		self.flag_run_num -= 1
		if self.id_data_chan != None:
			self.obj_container.datIN_ChanDel(self.id_data_chan)
		self.id_data_chan = None


