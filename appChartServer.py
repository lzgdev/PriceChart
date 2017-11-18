
import time
import os.path

from threading import Lock
from flask import Flask, render_template, send_from_directory
from flask import session, request

import flask_socketio
from flask_socketio import SocketIO, emit, disconnect
from flask_socketio import rooms, join_room, leave_room, close_room

from pymongo  import MongoClient
from adapters import TradeBook_DbBase

# Set this variable to "threading", "eventlet" or "gevent" to test the
# different async modes, or leave it set to None for the application to choose
# the best option based on installed packages.
async_mode = None

##
app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()

SIO_NAMESPACE = '/bfx.charts'

def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        socketio.sleep(10)
        count += 1
        socketio.emit('my_response',
                      {'data': 'Server generated event', 'count': count},
                      namespace=SIO_NAMESPACE)

# app data
appData_TB_P0 = TradeBook_DbBase("P0", 25)
db_client = MongoClient('localhost', 27017)
appData_TB_P0.dbInit(db_client['books'])
appData_TB_P0.dbLoad_Books()

@app.route('/js/<path:js_file>')
def def_static_js(js_file):
	js_code_path = 'code/js' if os.path.isfile('code/js/' + js_file) else app.static_folder + '/js'
	return send_from_directory(js_code_path, js_file, mimetype='application/javascript')

@app.route('/css/<path:css_file>')
def def_static_css(css_file):
	return send_from_directory(app.static_folder + '/css', css_file, mimetype='text/css')

@app.route('/demo/<path:filename>')
def def_static_demo(filename):
	return send_from_directory('demo/html', filename, mimetype='text/html')

@app.route('/depth/<path:filename>')
def def_active_depth(filename):
	jsvv_time = int(time.time()) * 1000;
	js11_ver = str(jsvv_time + 11)
	js12_ver = str(jsvv_time + 12)
	js19_ver = str(jsvv_time + 19)
	js21_ver = str(jsvv_time + 21)
	js22_ver = str(jsvv_time + 22)
	js29_ver = str(jsvv_time + 29)
	return render_template(filename,
					dev_js11_ver=js11_ver, dev_js12_ver=js12_ver, dev_js19_ver=js19_ver,
					dev_js21_ver=js21_ver, dev_js22_ver=js22_ver, dev_js29_ver=js29_ver)

@app.route("/")
@app.route("/index.html")
def def_static_index():
	return send_from_directory(app.static_folder + '/html', 'index.html')

class MyNamespace(flask_socketio.Namespace):
    def on_my_event(self, message):
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': message['data'], 'count': session['receive_count']})

    def on_my_broadcast_event(self, message):
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': message['data'], 'count': session['receive_count']},
             broadcast=True)

    def on_join(self, message):
        join_room(message['room'])
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': 'In rooms: ' + ', '.join(rooms()),
              'count': session['receive_count']})

    def on_leave(self, message):
        leave_room(message['room'])
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': 'In rooms: ' + ', '.join(rooms()),
              'count': session['receive_count']})

    def on_close_room(self, message):
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response', {'data': 'Room ' + message['room'] + ' is closing.',
                             'count': session['receive_count']},
             room=message['room'])
        close_room(message['room'])

    def on_my_room_event(self, message):
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': message['data'], 'count': session['receive_count']},
             room=message['room'])

    def on_disconnect_request(self):
        session['receive_count'] = session.get('receive_count', 0) + 1
        emit('my_response',
             {'data': 'Disconnected!', 'count': session['receive_count']})
        disconnect()

    def on_my_ping(self):
        emit('my_pong')

    def on_connect(self):
        global thread
        with thread_lock:
            if thread is None:
                thread = socketio.start_background_task(
                    target=background_thread)
        emit('my_response', {'data': 'Connected', 'count': 0})

    def on_disconnect(self):
        print('Client disconnected', request.sid)

socketio.on_namespace(MyNamespace(SIO_NAMESPACE))

if __name__ == '__main__':
    socketio.run(app, debug=True)

