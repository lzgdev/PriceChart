
import time
import json

from .datainput import CTDataInput_Http

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
		self.loc_mark_delay = 0
		self.loc_cnt_resp   = 0
		self.loc_run_chan   = 0
		self.loc_run_tmax   = -1
		self.loc_id_chan    = None
		self.loc_idx_chan   = None
		self.loc_name_chan  = None
		self.mts_rec_last   = 0

		#self.loc_run_tmax   = 3
		#self.flag_log_intv  = 1

	def onInit_HttpUrl_impl(self, url_parse):
		global dbg_tm_start
		if self.loc_mark_delay > 0:
			print("CTDataInput_HttpBfx::onInit_HttpUrl_impl, sleep", self.loc_mark_delay, "seconds.")
			time.sleep(self.loc_mark_delay)
			self.loc_mark_delay = 0
		tup_url = None
		if self.loc_run_chan >= len(self.obj_container.list_tups_datachan):
			return tup_url
		tup_chan = self.obj_container.list_tups_datachan[self.loc_run_chan]
		if self.loc_run_tmax >= 0:
			if self.loc_run_tmax == 0:
				return tup_url
			self.loc_run_tmax -= 1
		# try to add data channel
		if self.loc_idx_chan != self.loc_run_chan:
			self.num_chans += 1
			new_id_chan  = self.id_chan_off + self.num_chans
			new_idx_chan = self.obj_container.datIN_ChanAdd(new_id_chan, tup_chan[2], tup_chan[3])
			if new_idx_chan == self.loc_run_chan:
				self.loc_id_chan  = new_id_chan
				self.loc_idx_chan = new_idx_chan
		# try to load last doc from db
		rec_last = None
		if self.loc_idx_chan == self.loc_run_chan:
			rec_last = self.obj_container.list_tups_datachan[self.loc_idx_chan][1].getDoc_OutLast()
		if rec_last != None:
			mts_rec_new  = rec_last['mts']
			self.mts_rec_last = mts_rec_new if mts_rec_new >  self.mts_rec_last else (self.mts_rec_last + 1)
		# compose tup_url
		if   'trades' == tup_chan[2]:
			self.loc_name_chan  = tup_chan[2]
			url_suff   = '/trades/' + tup_chan[4]['symbol'] + '/hist'
			url_params = 'sort=1&start=' + str(self.mts_rec_last)
			if self.mts_rec_last == 0 and dbg_tm_start != None:
				url_params = 'sort=1&start=' + str(dbg_tm_start)
			tup_url = (url_suff, url_params)
		elif 'candles' == tup_chan[2]:
			self.loc_name_chan  = tup_chan[2]
			url_suff   = '/candles/' + tup_chan[4]['key'] + '/hist'
			url_params = 'sort=1&start=' + str(self.mts_rec_last)
			if self.mts_rec_last == 0 and dbg_tm_start != None:
				url_params = 'sort=1&start=' + str(dbg_tm_start)
			tup_url = (url_suff, url_params)
		else:
			tup_url = None
		if self.flag_log_intv >  0:
			#print("URL(impl2) , try:", self.loc_run_tmax, ", ret:", tup_url, ", tup:", tup_chan[2], tup_chan[3], ", parse:", url_parse)
			print("URL init2, try:", self.loc_run_tmax, ", ret:", tup_url, ", tup_chan(2-3):", (tup_chan[2], tup_chan[3]))
		return tup_url

	def onNcEV_HttpResponse_impl(self, status_code, content_type, http_data):
		global num_bfx_trades_recs, num_bfx_candles_recs
		#print("Resp, status:", status_code, ", Content-Type:", content_type)
		#print("data:", http_data)
		flag_data_valid = False
		flag_rate_lim = False
		flag_chan_end =  True
		obj_data  = None
		if content_type == "application/json; charset=utf-8" or content_type == "application/json":
			try:
				obj_data  = json.loads(http_data.decode('utf-8'))
			except:
				obj_data  = None
		len_list  = len(obj_data) if isinstance(obj_data, list) else -1
		if   len_list >= 1 and 'error' != obj_data[0]:
			if self.flag_log_intv >  1:
				#print("data:", obj_data)
				tm_last = self.mts_rec_last
				for item in obj_data:
					if    'trades' == self.loc_name_chan:
						tm_rec = item[1]
					elif 'candles' == self.loc_name_chan:
						tm_rec = item[0]
					print("data, diff:", tm_rec - tm_last, ", mts:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(tm_rec/1000))), ", item:", item)
			if self.flag_log_intv >  0:
				print("Data, resp:", self.loc_cnt_resp, ", len:", len_list)
			flag_data_valid =  True
			self.obj_container.datIN_DataFwd(self.loc_id_chan, DFMT_BFXV2, [self.loc_id_chan, obj_data])
			if    'trades' == self.loc_name_chan:
				if len_list >=  num_bfx_trades_recs:
					flag_chan_end = False
			elif 'candles' == self.loc_name_chan:
				if len_list >= num_bfx_candles_recs:
					flag_chan_end = False
			self.loc_cnt_resp += 1
		elif len_list >= 1 and 'error' == obj_data[0]:
			print("Error, len:", len_list, ", data:", obj_data)
			if len_list >= 2 and 11010 == obj_data[1]:
				flag_rate_lim =  True
		else:
			print("Error, code:", status_code, ", type:", content_type, ", data:", http_data)
		# clean channel data if necessary
		if   flag_rate_lim:
			print("Warning, rate limit:", str(obj_data))
			self.loc_mark_delay = 90
		elif flag_chan_end:
			print("Warning, data end:", status_code, http_data)
			self.loc_run_chan  += 1
			self.loc_id_chan    = None
			self.loc_idx_chan   = None
			self.loc_name_chan  = None
			self.mts_rec_last   = 0
		return flag_data_valid

