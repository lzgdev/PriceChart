
import time
import json

import urllib.parse

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

		self.loc_dbg_tmax   = -1
		self.num_chan_cfg   = -1
		self.run_chan_cfg   = 0

		# (0:idx_cfg, 1:id_chan, 2:idx_chan, 3:name_chan, 4:wreq_args, 5:dict_args)
		self.tup_run_stat   = ( -1, )

		self.loc_cnt_resp   = 0
		self.mts_req_start  = 0

		#self.loc_dbg_tmax   = 3
		#self.flag_log_intv  = 1

	def mark_ChanEnd(self):
		self.onMark_ChanEnd_impl()

	def getMts_ReqStart(self):
		return self.onMts_ReqStart_impl()

	def onLoop_ReadPrep_impl(self):
		global dbg_tm_start
		# update self.loc_dbg_tmax, try maximum self.loc_dbg_tmax times
		if self.loc_dbg_tmax >= 0:
			if self.loc_dbg_tmax == 0:
				return False
			self.loc_dbg_tmax -= 1
		# init self.num_chan_cfg
		if self.num_chan_cfg <  0 and isinstance(self.list_chan_cfg, list):
			self.num_chan_cfg = len(self.list_chan_cfg)

		self.mts_req_start  = -1
		while self.run_chan_cfg <  self.num_chan_cfg:
			# init data channel
			if self.tup_run_stat[0] != self.run_chan_cfg:
				self.onLoop_ReadPrep_chan_new(self.run_chan_cfg)
			if self.tup_run_stat[0] != self.run_chan_cfg:
				return False
			self.mts_req_start = self.getMts_ReqStart()
			if self.mts_req_start >= 0:
				break
			self.run_chan_cfg += 1
		if self.mts_req_start <  0:
			return False
		if self.mts_req_start == 0 and dbg_tm_start != None:
			self.mts_req_start  = dbg_tm_start

		# compose self.url_main_netloc and self.url_main_path
		url_parse  = urllib.parse.urlparse(self.url_http_pref)
		self.url_main_netloc  = url_parse.netloc
		if   'trades' == self.tup_run_stat[3]:
			url_suff   = '/trades/' + self.tup_run_stat[5]['symbol'] + '/hist'
			url_params = 'sort=1&start=' + str(self.mts_req_start)
			self.url_main_path   = url_parse.path + url_suff + '?' + url_params
		elif 'candles' == self.tup_run_stat[3]:
			url_suff   = '/candles/' + self.tup_run_stat[5]['key'] + '/hist'
			url_params = 'sort=1&start=' + str(self.mts_req_start)
			self.url_main_path   = url_parse.path + url_suff + '?' + url_params
		else:
			self.url_main_path   = None
		if self.flag_log_intv >  0:
			print("URL init2, try:", self.loc_dbg_tmax, ", ret:", self.url_main_path)
		# sleep for a while
		if self.loc_mark_delay > 0:
			print("CTDataInput_HttpBfx::onLoop_ReadPrep_impl, sleep", self.loc_mark_delay, "seconds.")
			time.sleep(self.loc_mark_delay)
			self.loc_mark_delay = 0
		#time.sleep(6)
		time.sleep(4)
		return False if self.url_main_path == None else True

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
			flag_data_valid =  True
			#self.obj_container.datIN_DataFwd(self.tup_run_stat[1], DFMT_BFXV2, [self.tup_run_stat[1], obj_data])
			id_chan = self.tup_run_stat[1]
			for idx_item in range(len_list):
				obj_item = obj_data[idx_item]
				if self.flag_log_intv >  1:
					if    'trades' == self.tup_run_stat[3]:
						tm_rec = obj_item[1]
					elif 'candles' == self.tup_run_stat[3]:
						tm_rec = obj_item[0]
					print("Data, diff:", tm_rec - self.mts_req_start, ", mts:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(round(tm_rec/1000))), ", item:", obj_item)
				self.obj_container.datIN_DataFwd(id_chan, DFMT_BFXV2, [id_chan, obj_item])
			if self.flag_log_intv >  0:
				print("Data, resp:", self.loc_cnt_resp, ", len:", len_list)
			if    'trades' == self.tup_run_stat[3]:
				if len_list >=  num_bfx_trades_recs:
					flag_chan_end = False
			elif 'candles' == self.tup_run_stat[3]:
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
			self.mark_ChanEnd()
		return flag_data_valid

	def onLoop_ReadPrep_chan_new(self, run_chan):
		cfg_chan  = self.list_chan_cfg[run_chan]
		name_chan = cfg_chan.get('channel', None)
		map_chan  = self.obj_container._gmap_TaskChans_chan(name_chan,
								cfg_chan.get('wreq_args', None))
		if map_chan == None:
			return False
		wreq_args_map = map_chan['wreq_args']
		dict_args_map = map_chan['dict_args']
		# try to add data channel
		tup_chan = self.obj_container.datIN_ChanGet(name_chan, wreq_args_map)
		if tup_chan != None and tup_chan[0] != None:
			id_chan  = tup_chan[0]
			idx_chan = tup_chan[1]
		else:
			CTDataInput_HttpBfx.num_chans += 1
			id_chan   = CTDataInput_HttpBfx.id_chan_off + CTDataInput_HttpBfx.num_chans
			idx_chan  = self.obj_container.datIN_ChanAdd(id_chan, name_chan, wreq_args_map)
			if idx_chan <  0:
				id_chan = None
		if id_chan != None:
			self.tup_run_stat = (run_chan, id_chan, idx_chan, name_chan, wreq_args_map, dict_args_map)
		return True if self.tup_run_stat[0] == run_chan else False

	def onMark_ChanEnd_impl(self):
		pass

	def onMts_ReqStart_impl(self):
		return -1


