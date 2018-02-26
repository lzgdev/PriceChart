
import os

import json
import copy
import logging

import tornado.websocket

import ktstat

class WebSockHandler(tornado.websocket.WebSocketHandler):
	def __init__(self, application, request, **kwargs):
		super(WebSockHandler, self).__init__(application, request, **kwargs)
		self.logger = logging.getLogger()
		self.obj_container = None
		self.pid_this = None

	def check_origin(self, origin):
		self.logger.info("WebSockHandler(chk): origin=" + origin)
		return True

	def open(self, ws_file):
		self.pid_this = os.getpid()
		self.logger.info("WebSockHandler: open file=" + ws_file + ", pid=" + str(self.pid_this))
		if self.obj_container == None:
			self.obj_container = ktstat.CTDataContainer_StatOut(self.logger, self)
			self.obj_container.flag_out_wsbfx =  True
		self.write_message({ 'event': 'info', 'version': 2, 'ext': 'KKAIEX02', })

	def on_close(self):
		self.logger.info("WebSockHandler: close");

	def on_message(self, message):
		try:
			obj_msg  = json.loads(message)
			evt_msg  = obj_msg['event']
		except:
			evt_msg  = None
		if evt_msg == None:
			return
		self.logger.info("WebSockHandler(msg): evt=" + evt_msg + ", obj_msg=" + str(obj_msg))
		if evt_msg == 'subscribe':
			self.onMsg_sbsc(evt_msg, obj_msg)

	def onMsg_sbsc(self, evt_msg, obj_msg):
		name_channel = obj_msg.get('channel', None)
		self.logger.info("WebSockHandler(sbsc): chan=" + str(name_channel) + ", args=" + str(obj_msg))
		if 'subscribe' == evt_msg:
			cfg_url   = 'mongodb://127.0.0.1:27017/bfx-down'
			#cfg_chan  = { 'load_args': { 'limit': 256, 'sort': [('$natural', 1)], }, }
			cfg_chan  = { 'load_args': { 'limit':  64, 'sort': [('$natural', 1)], }, }
			dict_args = { }
			for req_key, req_val in obj_msg.items():
				if req_key ==   'event':
					continue
				if req_key == 'channel':
					continue
				dict_args[req_key] = req_val
			cfg_chan['channel']   = name_channel
			cfg_chan['wreq_args'] = json.dumps(dict_args)
			self.obj_container.execMain(url=cfg_url, chans=[ cfg_chan, ])


