
import os

import json
import copy
import logging

import tornado.websocket

from .datacontainer_wsbfx       import CTDataContainer_WsBfxOut

class WebSockHandler(tornado.websocket.WebSocketHandler):
	id_chan_mark = 1

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
		id_chan_new = self.id_chan_mark
		self.id_chan_mark += 1
		self.logger.info("WebSockHandler: open file=" + ws_file + ", pid=" + str(self.pid_this))
		if self.obj_container == None:
			self.obj_container = CTDataContainer_WsBfxOut(self.logger)
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
		wreq_args = copy.copy(obj_msg)
		try:
			chan_msg = wreq_args['channel']
		except:
			chan_msg = None
		self.logger.info("WebSockHandler(sbsc): chan=" + str(chan_msg) + ", wreq_args=" + str(wreq_args))
		#filt_args = { 'channel': chan_msg, }
		#filt_args = { 'coll': { '$regex': 'candles-1m-.*', } }
		filt_args = { 'channel': chan_msg, 'reqargs': KTDataMedia_DbReader_WsOut.wreq_args2str(wreq_args), }
		name_coll = None if obj_doc == None else obj_doc['coll']
		wret_args = copy.copy(wreq_args)
		wret_args.update({ 'event': 'subscribed', 'channel': chan_msg, 'chanId': id_chan_sbsc, })
		self.write_message(wret_args)
		self.logger.info("WebSockHandler(sbsc): chan=" + str(chan_msg) + ", name=" + str(name_coll) + ", obj_doc=" + str(obj_doc))
		if   chan_msg == 'book':
			obj_dataset = CTDataSet_ABooks_WsOut(self.logger, wreq_args, self, id_chan_sbsc)
		elif chan_msg == 'candles':
			obj_dataset = CTDataSet_ACandles_WsOut(512, self.logger, wreq_args, self, id_chan_sbsc)
		else:
			obj_dataset = None

		if self.obj_container == None:


"""
import ktdata


class CTDataSet_ABooks_WsOut(ktdata.CTDataSet_ABooks_Adapter):
	def __init__(self, logger, wreq_args, obj_ws, id_ws_chan):
		super(CTDataSet_ABooks_WsOut, self).__init__(logger, wreq_args)
		#self.flag_dbg_rec =  True
		self.obj_ws  = obj_ws
		self.id_ws_chan = id_ws_chan

	def onLocDataSync_CB(self):
		out_docs = []
		for book_rec in self.loc_book_bids:
			out_docs.append([ book_rec['price'], book_rec['count'], 0.0 + book_rec['amount'], ])
		for book_rec in self.loc_book_asks:
			out_docs.append([ book_rec['price'], book_rec['count'], 0.0 - book_rec['amount'], ])
		self.obj_ws.write_message(str([self.id_ws_chan, out_docs]))

	def onLocRecAdd_CB(self, flag_plus, book_rec, flag_bids, idx_book, flag_del):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ABooks_DbIn(onLocRecAdd_CB): rec=" + str(book_rec))
		if not flag_plus:
			return False
		type_rec = book_rec['type']
		out_doc  = [ book_rec['price'], book_rec['count'],
					book_rec['amount'] if type_rec == 'bid' else (0.0 - book_rec['amount']), ]
		self.obj_ws.write_message(str([self.id_ws_chan, out_doc]))
		return True

class CTDataSet_ACandles_WsOut(ktdata.CTDataSet_ACandles_Adapter):
	def __init__(self, recs_size, logger, wreq_args, obj_ws, id_ws_chan):
		super(CTDataSet_ACandles_WsOut, self).__init__(recs_size, logger, wreq_args)
		#self.flag_dbg_rec   =  True
		self.obj_ws  = obj_ws
		self.id_ws_chan = id_ws_chan

	def onLocDataClean_CB(self):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ACandles_WsOut(onLocDataClean_CB): ...")

	def onLocDataSync_CB(self):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ACandles_WsOut(onLocDataSync_CB): ...")

	def onLocRecAdd_CB(self, flag_plus, candle_rec, rec_index):
		if self.flag_dbg_rec:
			self.logger.info("CTDataSet_ABooks_DbIn(onLocRecAdd_CB): rec=" + str(candle_rec))
		out_doc = [ candle_rec['mts'], candle_rec['open'],
					candle_rec['close'], candle_rec['high'],
					candle_rec['low'], candle_rec['volume'], ]
		self.obj_ws.write_message(str([self.id_ws_chan, out_doc]))

"""

