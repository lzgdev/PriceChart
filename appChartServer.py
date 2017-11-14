
import time

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
	return send_from_directory(app.static_folder + '/js', js_file, mimetype='application/javascript')

@app.route('/css/<path:css_file>')
def def_static_css(css_file):
	return send_from_directory(app.static_folder + '/css', css_file, mimetype='text/css')

@app.route('/demo/<path:filename>')
def def_static_demo(filename):
	return send_from_directory('demo/html', filename, mimetype='text/html')

@app.route('/depth/<path:filename>')
def def_active_depth(filename):
	js01_ver = str(int((time.time()+1) * 1000000))
	js02_ver = str(int((time.time()+2) * 1000000))
	js03_ver = str(int((time.time()+3) * 1000000))
	return render_template(filename, dev_js01_ver=js01_ver, dev_js02_ver=js02_ver, dev_js03_ver=js03_ver,)

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

