
import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.websocket

import os.path

gDir_root = os.path.dirname(__file__)

gDir_ui_static = os.path.join(gDir_root, 'ui/static')

class MainHandler(tornado.web.RequestHandler):
	def initialize(self, code_entry):
		self.code_entry = code_entry

	def get(self):
		path_pref = None
		path_file = os.path.basename(self.request.path)
		if   self.code_entry == 'home':
			return self.render('index.html')
		elif self.code_entry == 'views':
			path_pref = 'views'
		#return self.render(os.path.join(path_pref, path_file), dev_js29_ver=1122)
		return self.render(os.path.join(path_pref, path_file),
								dev_js29_ver=1122)


class HomeHandler(tornado.web.StaticFileHandler):
	def validate_absolute_path(self, root, absolute_path):
		if self.request.uri == '/':
			absolute_path = os.path.join(absolute_path, "html")
		print("HomeHandler: root=" + root + ", absolute_path=" + absolute_path + ", request=" + str(self.request))
		return super(HomeHandler, self).validate_absolute_path(root, absolute_path)

class DemoHandler(tornado.web.RequestHandler):
	def get(self):
		path_file = os.path.basename(self.request.path)
		return self.render(path_file)

	def get_template_path(self):
		return os.path.join(gDir_root, 'demo/html')

class MiscHandler(tornado.web.StaticFileHandler):
	def validate_absolute_path(self, root, absolute_path):
		path_file = os.path.basename(self.request.path)
		#print("MiscHandler: root=" + root + ", absolute_path=" + absolute_path + ", request=" + str(self.request))
		if os.path.isfile(os.path.join(gDir_root, 'code/js', path_file)):
			return os.path.join(gDir_root, 'code/js', path_file)
		return super(MiscHandler, self).validate_absolute_path(root, absolute_path)

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
		settings = dict(
			debug=True,
			template_path=os.path.join(gDir_root, 'ui/templates'),
			static_path=os.path.join(gDir_root, 'ui/static'),
		)
		handlers = [
			(r'/views/.*\.html', MainHandler, { 'code_entry': 'views', }),
			(r'/demo/.*\.html',  DemoHandler),
			(r'/ws', SocketHandler),
			(r'/(css/.+\.css)', MiscHandler, { 'path': settings['static_path'] }),
			(r'/(js/.+\.js)',   MiscHandler, { 'path': settings['static_path'] }),
			(r'/(favicon.ico)', MiscHandler, { 'path': settings['static_path'] }),
			#(r'/(favicon.ico)', tornado.web.StaticFileHandler, dict(path=settings['static_path'])),
			#(r'/(.*)',           HomeHandler, { 'path': 'ui/static', 'default_filename': 'index.html', }),
			(r'/()',           HomeHandler, { 'path': 'ui/static', 'default_filename': 'index.html', }),
		]
		super(AppChartServer, self).__init__(handlers, **settings)

def main():
	app = AppChartServer()
	app.listen(8888)
	tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
	main()

