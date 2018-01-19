
import os
import websocket

class CTDataInput(object):
	def __init__(self, logger, obj_container):
		object.__init__(self)
		self.logger   = logger
		self.obj_container = obj_container
		self.pid_this = os.getpid()
		self.inf_this = 'DataInput(pid=' + str(self.pid_this) + ')'
		self.flag_log_intv = False

class CTDataInput_Ws(CTDataInput, websocket.WebSocketApp):
	def __init__(self, logger, obj_container, url_ws):
		CTDataInput.__init__(self, logger, obj_container)
		websocket.WebSocketApp.__init__(self, url_ws)
		self.on_open  = CTDataInput_Ws.ncEV_Open
		self.on_message = CTDataInput_Ws.ncEV_Message
		self.on_error = CTDataInput_Ws.ncEV_Error
		self.on_close = CTDataInput_Ws.ncEV_Close

	def ncOP_Exec():
		self.onNcOP_Exec_impl()

	def ncEV_Open(self):
		self.onNcEV_Open_impl()

	def ncEV_Message(self, message):
		self.onNcEV_Message_impl(message)

	def ncEV_Error(self, error):
		self.onNcEV_Error_impl(error)

	def ncEV_Close(self):
		self.onNcEV_Close_impl()

	def onNcOP_Exec_impl():
		pass

	def onNcEV_Open_impl(self):
		if self.flag_log_intv:
			self.logger.info(self.inf_this + " websocket Opened!")

	def onNcEV_Message_impl(self, message):
		pass

	def onNcEV_Error_impl(self, error):
		self.logger.error(self.inf_this + " websocket Error: " + str(error))

	def onNcEV_Close_impl(self):
		if self.flag_log_intv:
			self.logger.info(self.inf_this + " websocket Closed!")

