
import tornado.ioloop
import tornado.web
import tornado.websocket

import os.path

gDir_root = os.path.dirname(__file__)
gDir_ui_static = 'ui/static'
gDir_demo_html = 'demo/html'

class MainHandler(tornado.web.RequestHandler):
	def initialize(self, code_entry):
		self.code_entry = code_entry

	def get(self):
		path_pref = None
		path_file = os.path.basename(self.request.path)
		if   self.code_entry == 'home':
			return self.render(gDir_ui_static + '/html/index.html')
		elif self.code_entry == 'views':
			path_pref = 'ui/html/views'
		return self.render(os.path.join(path_pref, path_file),
								dev_js29_ver=1122)

class DemoHandler(tornado.web.RequestHandler):
	def get(self):
		path_file = os.path.basename(self.request.path)
		return self.render(os.path.join(gDir_demo_html, path_file))

class MiscHandler(tornado.web.RequestHandler):
	def initialize(self, mime_type):
		self.mime_type = mime_type

	def get(self):
		path_pref = None
		path_file = os.path.basename(self.request.path)
		if   self.mime_type == 'text/css':
			path_pref = gDir_ui_static + '/css/'
		elif self.mime_type == 'text/javascript':
			if   os.path.isfile(os.path.join(gDir_root, 'code/js', path_file)):
				path_pref = 'code/js'
			else:
				path_pref = os.path.join(gDir_ui_static, 'js')
		return self.render(os.path.join(path_pref, path_file))

class SocketHandler(tornado.websocket.WebSocketHandler):
	def check_origin(self, origin):
		return True

	def open(self):
		if self not in cl:
			cl.append(self)

	def on_close(self):
		if self in cl:
			cl.remove(self)

class AppChartServer(tornado.web.Application):
	def __init__(self):
		handlers = [
			(r'/',               MainHandler, { 'code_entry': 'home', }),
			(r'/views/.*\.html', MainHandler, { 'code_entry': 'views', }),
			(r'/demo/.*\.html',  DemoHandler),
			(r'/ws', SocketHandler),
			(r'/css/.*\.css', MiscHandler, { 'mime_type': 'text/css', }),
			(r'/js/.*\.js',   MiscHandler, { 'mime_type': 'text/javascript', }),
			(r'/(favicon.ico)', tornado.web.StaticFileHandler, { 'path': gDir_ui_static + '/icons', }),
		]
		super(AppChartServer, self).__init__(handlers)

def main():
	app = AppChartServer()
	app.listen(8888)
	tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
	main()

