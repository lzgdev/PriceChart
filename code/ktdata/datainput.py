
import os

import websocket

import http.client
import urllib.parse

class CTDataInput(object):
	def __init__(self, logger, obj_container):
		object.__init__(self)
		self.logger   = logger
		self.obj_container = obj_container
		self.pid_this = os.getpid()
		self.inf_this = 'DataInput(pid=' + str(self.pid_this) + ')'
		self.flag_log_intv = 0


class CTDataInput_Ws(CTDataInput, websocket.WebSocketApp):
	def __init__(self, logger, obj_container, url_ws):
		CTDataInput.__init__(self, logger, obj_container)
		websocket.WebSocketApp.__init__(self, url_ws)
		self.on_open  = CTDataInput_Ws.ncEV_Open
		self.on_message = CTDataInput_Ws.ncEV_Message
		self.on_error = CTDataInput_Ws.ncEV_Error
		self.on_close = CTDataInput_Ws.ncEV_Close

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

	def exec_HttpExec(self):
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

	def onExec_HttpGet_impl(self, url_netloc, url_path):
		# compose real http request url
		if self.flag_log_intv >  1:
			print("URL, netloc:", url_netloc, ", path:", url_path)
		http_conn = http.client.HTTPSConnection(url_netloc)
		http_conn.request("GET", url_path)
		http_resp = http_conn.getresponse()
		content_type = http_resp.getheader('Content-Type')
		if self.flag_log_intv >  1:
			print("Resp, Status:", http_resp.status, ", reason:", http_resp.reason, ", Content-Type:", content_type)
		http_data = None
		if content_type != None:
			http_data = http_resp.read()
		if self.flag_log_intv >  1:
			print("Resp, Data:", http_data)
		http_conn.close()
		self.onNcEV_HttpResponse_impl(http_resp.status, content_type, http_data)

	def onInit_HttpUrl_impl(self, url_parse):
		return None

	def onNcEV_HttpResponse_impl(self, status_code, content_type, http_data):
		pass

