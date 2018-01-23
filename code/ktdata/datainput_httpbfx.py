
import time
import json

from .datainput import CTDataInput_Http
from .datacontainer_db  import CTDataContainer_DbOut

from .dataset   import DFMT_KKAIPRIV, DFMT_BFXV2, MSEC_TIMEOFFSET

# v1 time 1516640100
# v2 time 1516640100000
dbg_tm_start = None
#dbg_tm_start = 1516640100000

num_bfx_trades_recs  = 120
num_bfx_candles_recs = 120

class CTDataInput_HttpBfx(CTDataInput_Http):
	id_chan_off = round(time.time() * 1000)
	num_chans   = 0

	def __init__(self, logger, obj_container, url_http_pref):
		CTDataInput_Http.__init__(self, logger, obj_container, url_http_pref)
		self.loc_run_chan   = 0
		self.loc_run_tmax   = -1
		self.loc_id_chan    = None
		self.loc_idx_chan   = None
		self.loc_name_chan  = None

		#self.loc_run_tmax   = 3
		#self.flag_log_intv  = 1

	def onInit_HttpUrl_impl(self, url_parse):
		global dbg_tm_start
		tup_url = None
		if self.loc_run_chan >= len(self.obj_container.list_tups_datachn):
			return tup_url
		tup_chan = self.obj_container.list_tups_datachn[self.loc_run_chan]
		if self.loc_run_tmax >= 0:
			if self.loc_run_tmax == 0:
				return tup_url
			self.loc_run_tmax -= 1
		# try to add data channel
		if self.loc_idx_chan != self.loc_run_chan:
			self.num_chans += 1
			new_id_chan  = self.id_chan_off + self.num_chans
			new_idx_chan = self.obj_container.datIN_ChanAdd(new_id_chan, tup_chan[1], tup_chan[2])
			if new_idx_chan == self.loc_run_chan:
				self.loc_id_chan  = new_id_chan
				self.loc_idx_chan = new_idx_chan
		# try to load last doc from db
		if isinstance(self.obj_container, CTDataContainer_DbOut) and self.loc_idx_chan == self.loc_run_chan:
			self.db_doc_last = self.obj_container.list_dbad_dataout[self.loc_idx_chan].db_doc_last
		else:
			self.db_doc_last = None
		# compose tup_url
		if   'trades' == tup_chan[1]:
			self.loc_name_chan  = tup_chan[1]
			url_suff   = '/trades/' + tup_chan[3]['symbol'] + '/hist'
			url_params = 'sort=1&start=' + (
					'0' if self.db_doc_last == None else str(self.db_doc_last['mts']))
			if self.db_doc_last == None and dbg_tm_start != None:
				url_params = 'sort=1&start=' + str(dbg_tm_start)
			tup_url = (url_suff, url_params)
		elif 'candles' == tup_chan[1]:
			self.loc_name_chan  = tup_chan[1]
			url_suff   = '/candles/' + tup_chan[3]['key'] + '/hist'
			url_params = 'sort=1&start=' + (
					'0' if self.db_doc_last == None else str(self.db_doc_last['mts']))
			if self.db_doc_last == None and dbg_tm_start != None:
				url_params = 'sort=1&start=' + str(dbg_tm_start)
			tup_url = (url_suff, url_params)
		else:
			tup_url = None
		if self.flag_log_intv >  0:
			print("URL(impl2) , try:", self.loc_run_tmax, ", ret:", tup_url, ", tup:", tup_chan[1], tup_chan[2], ", parse:", url_parse)
		return tup_url

	def onNcEV_HttpResponse_impl(self, status_code, content_type, http_data):
		global num_bfx_trades_recs, num_bfx_candles_recs
		#print("Resp, status:", status_code, ", Content-Type:", content_type)
		#print("data:", http_data)
		try:
			obj_data  = json.loads(http_data.decode('utf-8'))
		except:
			obj_data  = None
		if obj_data != None:
			if self.flag_log_intv >  0:
				#print("data:", obj_data)
				tm_last = 0 if self.db_doc_last == None else self.db_doc_last['mts']
				for item in obj_data:
					if    'trades' == self.loc_name_chan:
						tm_rec = item[1]
					elif 'candles' == self.loc_name_chan:
						tm_rec = item[0]
					print("diff:", tm_rec - tm_last, ", mts:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(tm_rec/1000))), ", item:", item)
				print("data len:", len(obj_data))
			self.obj_container.datIN_DataFwd(self.loc_id_chan, DFMT_BFXV2, [self.loc_id_chan, obj_data])
			if    'trades' == self.loc_name_chan:
				self.loc_run_chan += 1 if len(obj_data) <  num_bfx_trades_recs  else 0
			elif 'candles' == self.loc_name_chan:
				self.loc_run_chan += 1 if len(obj_data) <  num_bfx_candles_recs else 0

