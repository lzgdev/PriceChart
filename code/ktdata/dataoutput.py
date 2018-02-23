

class CTDataOutput(object):
	def __init__(self, logger):
		object.__init__(self)
		self.logger   = logger
		self.flag_dbg_rec   = -1

	# interfaces for DataContainer
	def getDoc_OutLast(self):
		return None

	def prepOutChan(self, **kwargs):
		return self.onPrep_OutChan_impl(**kwargs)

	def clrAppend(self, msec_now):
		return self.onClrAppend_impl(msec_now)

	def synAppend(self, msec_now):
		return self.onSynAppend_impl(msec_now)

	def docAppend(self, doc_rec):
		return self.onDocAppend_impl(doc_rec)

	# implementations for DataContainer
	def onPrep_OutChan_impl(self, **kwargs):
		return False

	def onClrAppend_impl(self, msec_now):
		return self.onOut_DatClear_impl()

	def onSynAppend_impl(self, msec_now):
		list_docs = self.onSynAppend_get_lists(msec_now)
		if not isinstance(list_docs, list):
			return 0
		dat_array = []
		for doc_rec in list_docs:
			dat_out = self.onTran_Doc2Dat_impl(doc_rec)
			if dat_out != None:
				dat_array.append(dat_out)
		return self.onOut_DatArray_impl(dat_array)

	def onDocAppend_impl(self, doc_rec):
		dat_out = self.onTran_Doc2Dat_impl(doc_rec)
		if dat_out == None:
			return 0
		return self.onOut_DatOne_impl(dat_out)

	# implementaions for data output(internal utility call)
	def onSynAppend_get_lists(self, msec_now):
		return None

	def onTran_Doc2Dat_impl(self, doc_rec):
		return doc_rec

	# implementaions for data output
	def onOut_DatClear_impl(self):
		return -1

	def onOut_DatOne_impl(self, dat_one):
		return 0

	def onOut_DatArray_impl(self, dat_array):
		return 0


