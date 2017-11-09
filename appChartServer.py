from flask import Flask, render_template
from flask_socketio import SocketIO

##
from flask import render_template, send_from_directory
app = Flask(__name__, static_folder='static')

#app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

@app.route('/js/<path:js_file>')
def def_static_js(js_file):
	return send_from_directory(app.static_folder + '/js', js_file, mimetype='application/javascript')

@app.route('/css/<path:css_file>')
def def_static_css(css_file):
	return send_from_directory(app.static_folder + '/css', css_file, mimetype='text/css')

@app.route('/demo/<path:filename>')
def def_static_demo(filename):
	return send_from_directory('demo', filename, mimetype='text/html')

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

@socketio.on('my event')
def handle_my_custom_event(json):
	print('received my event: ' + str(json))
	socketio.emit('my response', json)

if __name__ == '__main__':
	socketio.run(app)

