
import json
import logging

import tornado.websocket

import ktdata

class WebSockHandler(tornado.websocket.WebSocketHandler):
	id_chan_mark = 1

	def __init__(self, application, request, **kwargs):
		super(WebSockHandler, self).__init__(application, request, **kwargs)
		self.logger = logging.getLogger()
		self.obj_db_reader = None

	def check_origin(self, origin):
		self.logger.info("WebSockHandler(chk): origin=" + origin)
		return True

	def open(self, ws_file):
		id_chan_new = self.id_chan_mark
		self.id_chan_mark += 1
		self.logger.info("WebSockHandler: open file=" + ws_file)
		self.obj_db_reader = KTDataMedia_DbReader_ws(self.logger, self)
		self.obj_db_reader.dbOP_Connect('mongodb://127.0.0.1:27017', 'bfx-pub')
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
			self.onMsg_sbsc(obj_msg)

	def onMsg_sbsc(self, obj_msg):
		self.id_chan_mark += 1
		id_chan_sbsc = self.id_chan_mark
		try:
			chan_msg = obj_msg['channel']
		except:
			chan_msg = None
		self.logger.info("WebSockHandler(sbsc): chan=" + str(chan_msg) + ", obj_msg=" + str(obj_msg))
		filt_wreq_args = { }
		for req_key, req_val in obj_msg.items():
			if req_key == 'event'  or req_key == 'channel':
				continue
			filt_wreq_args[req_key] = req_val
		filt_args = { 'channel': chan_msg, 'reqargs': filt_wreq_args, }
		#obj_doc   = self.obj_db_reader.dbOP_DocFind_One(ktdata.COLLNAME_CollSet, { 'coll': { '$regex': 'candles-1m-.*', } }, [('$nature', -1)])
		obj_doc   = self.obj_db_reader.dbOP_DocFind_One(ktdata.COLLNAME_CollSet, filt_args, [('$nature', -1)])
		name_coll = None if obj_doc == None else obj_doc['coll']
		wret_args = { 'event': 'subscribed', 'channel': chan_msg, 'chanId': id_chan_sbsc, }
		wret_args.update(filt_wreq_args)
		self.write_message(wret_args)
		#self.logger.info("WebSockHandler(sbsc): chan=" + str(chan_msg) + ", name=" + str(name_coll) + ", obj_doc=" + str(obj_doc))
		self.obj_db_reader.dbOP_CollLoad(id_chan_sbsc, name_coll, {}, None)


class KTDataMedia_DbReader_ws(ktdata.KTDataMedia_DbReader):
	def __init__(self, logger, obj_ws):
		super(KTDataMedia_DbReader_ws, self).__init__(logger)
		self.obj_ws = obj_ws
		self.list_coll_docs = []

	def onDbEV_CollLoad_Begin(self, id_chan):
		#self.logger.info("DataMedia(load): begin, chan=" + str(id_chan))
		self.list_coll_docs.clear()

	def onDbEV_CollLoad_Doc(self, id_chan, obj_doc):
		#self.logger.info("DataMedia(load): chan=" + str(id_chan) + ", doc=" + str(obj_doc))
		#self.list_coll_docs.append(obj_doc)
		self.list_coll_docs.append([ obj_doc['mts'], obj_doc['open'],
					obj_doc['close'], obj_doc['high'], obj_doc['low'], obj_doc['volume'],])

	def onDbEV_CollLoad_End(self, id_chan, num_docs):
		#self.logger.info("DataMedia(load): end, chan=" + str(id_chan) + ", num=" + str(num_docs))
		self.obj_ws.write_message(str([id_chan, self.list_coll_docs,]))

