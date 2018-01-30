
import os

import json
import copy
import logging

import tornado.websocket

from .datacontainer_wsbfx       import CTDataContainer_WsBfxOut

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
			self.obj_container = CTDataContainer_WsBfxOut(self.logger, self)
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
		wreq_args = copy.copy(obj_msg)
		try:
			name_channel = wreq_args['channel']
		except:
			name_channel = None
		self.logger.info("WebSockHandler(sbsc): chan=" + str(name_channel) + ", wreq_args=" + str(wreq_args))
		#filt_args = { 'channel': name_channel, }
		#filt_args = { 'coll': { '$regex': 'candles-1m-.*', } }
		if 'subscribe' == evt_msg:
			self.obj_container.execMain(name_chan=name_channel, wreq_args=obj_msg)


