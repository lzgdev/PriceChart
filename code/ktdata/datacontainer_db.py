

from .datacontainer import CTDataContainer

class CTDataContainer_DbOut(CTDataContainer):
	def __init__(self, logger):
		CTDataContainer.__init__(self, logger)

	def datCB_DataClean(self, obj_dataset):
		pass

	def datCB_DataSync(self, obj_dataset):
		if obj_dataset != None:
			obj_dataset.onLocDataSync_CB()

	def datCB_RecPlus(self, obj_dataset, doc_rec, idx_rec):
		if obj_dataset != None:
			obj_dataset.onLocRecAdd_CB(True, doc_rec, idx_rec)

	def onDatIN_ChanAdd_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		pass

	def onDatIN_ChanDel_ext(self, idx_chan, id_chan, name_chan, dict_msg):
		pass

