
import json
import logging

import tornado.websocket

class WebSockHandler(tornado.websocket.WebSocketHandler):
	def __init__(self, application, request, **kwargs):
		super(WebSockHandler, self).__init__(application, request, **kwargs)
		self.logger = logging.getLogger()

	def check_origin(self, origin):
		return True

	def open(self, ws_file):
		self.logger.info("WebSockHandler: open file=" + ws_file)
		self.write_message({ 'event': 'info', 'version': 2, 'ext': 'KKAIEX02', })
		pass

	def on_close(self):
		self.logger.info("WebSockHandler: close");
		pass

	def on_message(self, message):
		try:
			obj_msg  = json.loads(message)
		except:
			obj_msg  = None
		self.logger.info("WebSockHandler: obj_msg=" + str(obj_msg))

#obj_msg={'event': 'subscribe', 'channel': 'candles', 'key': 'trade:1m:tBTCUSD'}

