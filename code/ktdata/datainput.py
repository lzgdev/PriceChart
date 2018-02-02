
import os
import time

import websocket

import http.client
import urllib.parse

from .dataset       import DFMT_KKAIPRIV, DFMT_BFXV2

class CTDataInput(object):
	def __init__(self, logger, obj_container):
		object.__init__(self)
		self.logger   = logger
		self.obj_container = obj_container
		self.pid_this = os.getpid()
		self.inf_this = 'Din(pid=' + str(self.pid_this) + ')'
		self.flag_log_intv = 0

	def prepRead(self, **kwargs):
		self.onPrep_Read_impl(**kwargs)

	def execReadLoop(self):
		self.onExec_ReadLoop_impl()

	def closeRead(self):
		self.onClose_Read_impl()

	def onPrep_Read_impl(self, **kwargs):
		pass

	def onExec_ReadLoop_impl(self):
		pass

	def onClose_Read_impl(self):
		pass


class CTDataInput_Ws(CTDataInput, websocket.WebSocketApp):
	def __init__(self, logger, obj_container, url_ws):
		CTDataInput.__init__(self, logger, obj_container)
		websocket.WebSocketApp.__init__(self, url_ws)
		self.on_open  = CTDataInput_Ws.ncEV_Open
		self.on_message = CTDataInput_Ws.ncEV_Message
		self.on_error = CTDataInput_Ws.ncEV_Error
		self.on_close = CTDataInput_Ws.ncEV_Close

		self.inf_this = 'DinWs(pid=' + str(self.pid_this) + ')'

	def onExec_ReadLoop_impl(self):
		self.run_forever()

	def ncEV_Open(self):
		self.onNcEV_Open_impl()

	def ncEV_Message(self, message):
		self.onNcEV_Message_impl(message)

	def ncEV_Error(self, error):
		self.onNcEV_Error_impl(error)

	def ncEV_Close(self):
		self.onNcEV_Close_impl()

	def onNcEV_Open_impl(self):
		if self.flag_log_intv >  0:
			self.logger.info(self.inf_this + " websocket Opened!")

	def onNcEV_Message_impl(self, message):
		pass

	def onNcEV_Error_impl(self, error):
		self.logger.error(self.inf_this + " websocket Error: " + str(error))

	def onNcEV_Close_impl(self):
		if self.flag_log_intv >  0:
			self.logger.info(self.inf_this + " websocket Closed!")


class CTDataInput_Http(CTDataInput):
	def __init__(self, logger, obj_container, url_http_pref):
		CTDataInput.__init__(self, logger, obj_container)
		self.url_http_pref = url_http_pref

		self.obj_httpconn  = None
		self.num_httprest  = 0

	def onExec_ReadLoop_impl(self):
		while True:
			url_parse  = urllib.parse.urlparse(self.url_http_pref)
			tup_http_req = self.onInit_HttpUrl_impl(url_parse)
			if tup_http_req == None:
				break
			url_path   = url_parse.path
			url_suff   = tup_http_req[0]
			url_params = tup_http_req[1]
			if url_suff != None and url_suff != '':
				url_path += url_suff
			if url_params != None and url_params != '':
				url_path += '?' + url_params
			self.onExec_HttpGet_impl(url_parse.netloc, url_path)
			#time.sleep(6)
			time.sleep(4)

	def onExec_HttpGet_impl(self, url_netloc, url_path):
		# compose real http request url
		if self.flag_log_intv >  1:
			print("URL, netloc:", url_netloc, ", path:", url_path)
		if self.obj_httpconn == None:
			self.obj_httpconn = http.client.HTTPSConnection(url_netloc)
			self.num_httprest = 0
		self.obj_httpconn.request("GET", url_path)
		http_resp = self.obj_httpconn.getresponse()
		content_type = http_resp.getheader('Content-Type')
		if self.flag_log_intv >  1:
			print("Resp, Status:", http_resp.status, ", reason:", http_resp.reason, ", Content-Type:", content_type)
		http_data = None
		if content_type != None:
			http_data = http_resp.read()
		if self.flag_log_intv >  1:
			print("Resp, Data:", http_data)
		ret_proc = self.onNcEV_HttpResponse_impl(http_resp.status, content_type, http_data)
		self.num_httprest += 1
		if not ret_proc or self.num_httprest >= 10:
			self.obj_httpconn.close()
			self.obj_httpconn = None

	def onInit_HttpUrl_impl(self, url_parse):
		return None

	def onNcEV_HttpResponse_impl(self, status_code, content_type, http_data):
		pass


class CTDataInput_Db(CTDataInput):
	gid_chan_now = 11

	def __init__(self, logger, obj_container):
		CTDataInput.__init__(self, logger, obj_container)
		self.obj_dbadapter  = None
		self.flag_rec_plus  = True
		self.list_tmp_docs  = []

		self.id_data_chan   = None
		self.loc_name_chan  = None
		self.loc_wreq_args  = None

		self.flag_run_num   = 1

	def datFwd_Begin(self, id_chan):
		self.onDat_Begin_impl(id_chan)

	def datFwd_Doc(self, id_chan, obj_doc):
		self.onDat_FwdDoc_impl(id_chan, obj_doc)

	def datFwd_End(self, id_chan, num_docs):
		self.onDat_End_impl(id_chan, num_docs)

	def onExec_ReadLoop_impl(self):
		while True:
			ret_prep = self.onInit_DbPrep_impl()
			if not ret_prep:
				break
			self.onExec_DbRead_impl()

	def onInit_DbPrep_impl(self):
		return False

	def onExec_DbRead_impl(self):
		pass

	def onDat_Begin_impl(self, id_chan):
		#print("CTDataInput_Db::onDat_Begin_impl", id_chan)
		self.flag_rec_plus  =  True
		self.list_tmp_docs.clear()

	def onDat_End_impl(self, id_chan, num_docs):
		#print("CTDataInput_Db::onDat_End_impl", id_chan)
		self.flag_rec_plus  =  True
		self.list_tmp_docs.clear()

	def onDat_FwdDoc_impl(self, id_chan, obj_doc):
		type_rec = None if not 'type' in obj_doc else obj_doc['type']
		#print("CTDataInput_Db::onDat_FwdDoc_impl", id_chan, obj_doc)
		if   'reset' == type_rec:
			self.flag_rec_plus  = False
			self.list_tmp_docs.clear()
		elif  'sync' == type_rec:
			self.obj_container.datIN_DataFwd(id_chan, DFMT_KKAIPRIV, self.list_tmp_docs)
			self.flag_rec_plus  =  True
			self.list_tmp_docs.clear()
		else:
			if not self.flag_rec_plus:
				#print("CTDataInput_Db::onDat_FwdDoc_impl b=31", id_chan, obj_doc)
				self.list_tmp_docs.append(obj_doc)
			else:
				#print("CTDataInput_Db::onDat_FwdDoc_impl b=32", id_chan, obj_doc)
				self.obj_container.datIN_DataFwd(id_chan, DFMT_KKAIPRIV, obj_doc)


