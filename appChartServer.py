from flask import Flask, render_template
from flask_socketio import SocketIO

from pymongo  import MongoClient

from adapters import TradeBook_DbBase

##
from flask import render_template, send_from_directory
app = Flask(__name__, static_folder='static')

#app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

# app data
appData_TB_P0 = TradeBook_DbBase("P0", 25)
db_client = MongoClient('localhost', 27017)
appData_TB_P0.dbInit(db_client['books'])
appData_TB_P0.dbLoad_Books()

@app.route('/js/<path:js_file>')
def def_static_js(js_file):
	return send_from_directory(app.static_folder + '/js', js_file, mimetype='application/javascript')

@app.route('/css/<path:css_file>')
def def_static_css(css_file):
	return send_from_directory(app.static_folder + '/css', css_file, mimetype='text/css')

@app.route('/demo/<path:filename>')
def def_static_demo(filename):
	return send_from_directory('demo/html', filename, mimetype='text/html')

@app.route('/depth/<path:filename>')
def def_static_depth(filename):
	return send_from_directory('depth', filename, mimetype='text/html')

@app.route("/")
@app.route("/index.html")
def def_static_index():
	return send_from_directory(app.static_folder + '/html', 'index.html')

@socketio.on('message')
def handle_message(message):
	print('received message: ' + message)
	socketio.send(message)

@socketio.on('json')
def handle_json(json):
	print('received json: ' + str(json))
	socketio.send(json, json=True)

@socketio.on('js.req.books')
def handle_js_req_books(json):
	global appData_TB_P0
	print('received js.req.books: ' + str(json))
	#socketio.emit('js.ret.books', json)
	socketio.emit('js.ret.books', { 'bids': appData_TB_P0.loc_book_bids, 'asks': appData_TB_P0.loc_book_asks, })

if __name__ == '__main__':
	socketio.run(app)

