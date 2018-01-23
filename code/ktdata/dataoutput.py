

class CTDataOutput(object):
	def __init__(self, logger):
		object.__init__(self)
		self.logger   = logger

	def prepOutChan(self, **kwargs):
		return self.onPrep_OutChan_impl(**kwargs)

	def synAppend(self, msec_now):
		self.onSynAppend_impl(msec_now)

	def docAppend(self, doc_rec):
		doc_new = self.onDocAppend_impl(doc_rec)
		return doc_new

	def onPrep_OutChan_impl(self, **kwargs):
		return False

	def onSynAppend_impl(self, msec_now):
		pass

	def onDocAppend_impl(self, doc_rec):
		return None

