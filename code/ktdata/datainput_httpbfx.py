
import time
import json

from .datainput import CTDataInput_Http
from .datacontainer_db  import CTDataContainer_DbOut

from .dataset   import DFMT_KKAIPRIV, DFMT_BFXV2, MSEC_TIMEOFFSET

tm_bgn = None
tm_end = None

#tm_bgn = 1359056887000

# v1 time 1516509475
# v2 time 1516509148427

class CTDataInput_HttpBfx(CTDataInput_Http):
	id_chan_off = round(time.time() * 1000)
	num_chans   = 0

	def __init__(self, logger, obj_container, url_http_pref):
		CTDataInput_Http.__init__(self, logger, obj_container, url_http_pref)
		self.loc_run_chan   = 0
		self.loc_run_arg1   = 1
		self.loc_id_chan    = None
		self.loc_idx_chan   = None
		self.loc_name_chan  = None

		self.flag_log_intv  = 1

	def onInit_HttpUrl_impl(self, url_parse):
		global tm_bgn, tm_end
		tup_url = None
		if self.loc_run_chan >= len(self.obj_container.list_tups_datachn):
			return tup_url
		tup_chan = self.obj_container.list_tups_datachn[self.loc_run_chan]
		self.loc_run_arg1 -= 1
		if self.loc_run_arg1 <  0:
			return tup_url
		if   'trades' == tup_chan[1]:
			self.loc_name_chan  = tup_chan[1]
			url_suff   = '/trades/' + tup_chan[3]['symbol'] + '/hist'
			url_params = None
			if tm_bgn == None:
				tm_bgn = 0
			#url_params = 'sort=1'
			#url_params = 'sort=1&start=' + str(tm_bgn)
			#if tm_end != None:
			#	url_params = 'end=' + str(tm_end)
			tup_url = (url_suff, url_params)
		elif 'candles' == tup_chan[1]:
			self.loc_name_chan  = tup_chan[1]
			url_suff   = '/candles/' + tup_chan[3]['key'] + '/hist'
			url_params = None
			if tm_bgn == None:
				tm_bgn = 0
			url_params = 'sort=1&start=' + str(tm_bgn)
			#url_params = 'sort=1'
			tup_url = (url_suff, url_params)
		else:
			self.loc_name_chan  = None
		if tup_url != None and self.loc_id_chan == None:
			self.num_chans += 1
			self.loc_id_chan  = self.id_chan_off + self.num_chans
			self.loc_idx_chan = self.obj_container.datIN_ChanAdd(self.loc_id_chan, tup_chan[1], tup_chan[2])
		if isinstance(self.obj_container, CTDataContainer_DbOut) and self.loc_idx_chan != None:
			doc_last = self.obj_container.list_dbad_dataout[self.loc_idx_chan].db_doc_last
		if self.flag_log_intv >  0:
			print("URL(impl2) , try:", self.loc_run_arg1, ", ret:", tup_url, ", tup:", tup_chan[1], tup_chan[2], ", parse:", url_parse)
		return tup_url

	def onNcEV_HttpResponse_impl(self, status_code, content_type, http_data):
		global tm_bgn, tm_end
		if self.flag_log_intv >  0:
			print("Resp, status:", status_code, ", Content-Type:", content_type)
			print("Time, end:", tm_end, ", bgn:", time.ctime(round(tm_bgn/1000)))
		#print("data:", http_data)
		try:
			obj_data  = json.loads(http_data.decode('utf-8'))
		except:
			obj_data  = None
		if obj_data != None:
			#print("data:", obj_data)
			self.obj_container.datIN_DataFwd(self.loc_id_chan, DFMT_BFXV2, [self.loc_id_chan, obj_data])
			if tm_end == None:
				tm_end = 0
			for item in obj_data:
				if self.flag_log_intv >  0:
					#print("item:", item)
					if 'trades' == self.loc_name_chan:
						tm_rec = item[1]
					elif 'candles' == self.loc_name_chan:
						tm_rec = item[0]
					print("diff:", tm_end - tm_rec, ", item:", item, ", mts:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(tm_rec/1000))))
			if self.flag_log_intv >  0:
				print("data len:", len(obj_data))
			if   'trades' == self.loc_name_chan:
				tm_end = obj_data[len(obj_data)-1][1]
				tm_bgn = obj_data[len(obj_data)-1][1]
			elif 'candles' == self.loc_name_chan:
				tm_end = obj_data[len(obj_data)-1][0]
				tm_bgn = obj_data[len(obj_data)-1][0]

