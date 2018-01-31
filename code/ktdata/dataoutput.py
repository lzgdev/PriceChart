

class CTDataOutput(object):
	def __init__(self, logger):
		object.__init__(self)
		self.logger   = logger

	# interfaces for DataContainer
	def getDoc_OutLast(self):
		return None

	def prepOutChan(self, **kwargs):
		return self.onPrep_OutChan_impl(**kwargs)

	def clrAppend(self, msec_now):
		self.onClrAppend_impl(msec_now)

	def synAppend(self, msec_now):
		self.onSynAppend_impl(msec_now)

	def docAppend(self, doc_rec):
		doc_new = self.onDocAppend_impl(doc_rec)
		return doc_new

	# interfaces for internal data output call
	def tranDoc2Dat(self, doc_rec):
		return self.onTran_Doc2Dat_impl(doc_rec)

	def outDatOne(self, dat_one):
		return self.onOut_DatOne_impl(dat_one)

	def outDatArray(self, dat_array):
		return self.onOut_DatArray_impl(dat_array)

	# implementations for DataContainer
	def onPrep_OutChan_impl(self, **kwargs):
		return False

	def onClrAppend_impl(self, msec_now):
		pass

	def onSynAppend_impl(self, msec_now):
		pass

	def onDocAppend_impl(self, doc_rec):
		dat_out = None
		dat_one = self.tranDoc2Dat(doc_rec)
		if dat_one != None:
			dat_out = self.outDatOne(dat_one)
		return dat_out

	# implementaions for data output
	def onTran_Doc2Dat_impl(self, doc_rec):
		return doc_rec

	def onOut_DatOne_impl(self, dat_one):
		return None

	def onOut_DatArray_impl(self, dat_array):
		return None


