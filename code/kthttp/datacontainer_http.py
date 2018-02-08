
import json

import ktdata

from .datainput_dbreader    import CTDataInput_DbReader

from .dataoutput_wsbfx      import CTDataOut_WsBfx_stat01, \
							CTDataOut_WsBfx_ticker, CTDataOut_WsBfx_trades, CTDataOut_WsBfx_book, CTDataOut_WsBfx_candles


class CTDataContainer_HttpOut(ktdata.CTDataContainer):
	def __init__(self, logger, obj_outconn):
		ktdata.CTDataContainer.__init__(self, logger)
		self.obj_outconn = obj_outconn
		self.flag_out_wsbfx = False

	def onExec_Init_impl(self, list_task):
		for args_task in list_task:
			#print("CTDataContainer_HttpOut::onExec_Init_impl, task:", str(args_task))
			dbg_stat  = args_task.pop( 'dbg_stat', -1)
			name_chan = args_task.get('name_chan', None)
			name_src  = args_task.pop( 'name_src', None)
			idx_data_chan = self.addArg_DataChannel(name_chan, args_task['wreq_args'])
			#print("CTDataContainer_HttpOut::onExec_Init_impl, chan:", idx_data_chan, ", task:", str(args_task))
			if idx_data_chan >= 0:
				self.addObj_DataSource(CTDataInput_DbReader(self.logger, self), **args_task)
			#if idx_data_chan >= 0 and isinstance(self.list_tups_datachan[idx_data_chan][0], CTDataSet_Stat_Dbg):
			#	self.list_tups_datachan[idx_data_chan][0].dbg_stat = dbg_stat
		return None

	def onChan_DataOut_alloc(self, obj_dataset, name_chan, wreq_args, dict_args):
		#print("CTDataContainer_HttpOut::onChan_DataOut_alloc", name_chan, wreq_args)
		obj_dataout = None
		if   name_chan == 'ticker':
			obj_dataout = CTDataOut_WsBfx_ticker(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'trades':
			obj_dataout = CTDataOut_WsBfx_trades(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'book':
			obj_dataout = CTDataOut_WsBfx_book(self.logger, obj_dataset, self.obj_outconn)
		elif name_chan == 'candles':
			obj_dataout = CTDataOut_WsBfx_candles(self.logger, obj_dataset, self.obj_outconn)
		if obj_dataout == None:
			obj_dataout = super(CTDataContainer_HttpOut, self).onChan_DataOut_alloc(obj_dataset, name_chan, wreq_args, dict_args)
		return obj_dataout

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan):
		#print("CTDataContainer_HttpOut::onDatIN_ChanAdd_ext", idx_chan, id_chan)
		tup_chan  = self.list_tups_datachan[idx_chan]
		obj_dataset = tup_chan[0]
		obj_dataout = tup_chan[1]
		if obj_dataout != None:
			obj_dataout.prepOutChan(id_chan=id_chan, name_dbtbl=obj_dataset.name_dbtbl,
								name_chan=obj_dataset.name_chan, wreq_args=tup_chan[4])

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan):
		pass
