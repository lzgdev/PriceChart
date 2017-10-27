from flask import Flask
from flask import Response
from flask import request, jsonify
from flask import redirect, url_for
from flask import make_response
from flask import render_template, send_from_directory

import uuid, json
import os, sys, signal
import time
import urllib.parse

import threading

#import md5

#import kkaieval

sys.path.append(os.path.abspath('py-utils'))
#from u01_fs import *
#from u02_kkai import *


pid_http_up = 0

app = Flask(__name__, static_folder='static')

g_dir_httpsrv   = os.getcwd() + "/httpsrv"
g_dir_httpsrv_pub = g_dir_httpsrv + "/pub"
g_dir_httpsrv_tmp = g_dir_httpsrv + "/tmp"

g_dir_httpsrv_hbcam  = 'httpsrv/pub/kkai.cam'
g_dir_httpsrv_hbcalc = 'httpsrv/pub/kkai.calc'

_g_locdir_up    = g_dir_httpsrv_tmp + "/up"
_g_locdir_part  = g_dir_httpsrv_tmp + "/part"
_g_locdir_merge = g_dir_httpsrv_tmp + "/merge"

_g_lock_jsonfile = threading.Lock()

envdef_camrec_out_type     = 'depth'

def fork_http_up():
	global pid_http_up
	srv_app_name = "httpsrv-up"
	srv_app_path = os.path.realpath(os.getcwd() + "/../httpsrv-up/main/httpsrv-up")
	if not os.path.exists(srv_app_path):
		print("ERR: can't find http upload service app!")
		return
	pid_http_up = os.fork()
	if (pid_http_up == 0):
		print("INFO: invoke http upload service in child process(pid=%d) ..." % (os.getpid()))
		os.execlp(srv_app_path, srv_app_name, '-B', g_dir_httpsrv)
		os._exit(255)


def srv_get_fname_ext(file_type):
	if (file_type == "vid.cover"):
		return ".jpg"
	elif (file_type == "vid.file"):
		return ".mp4"
	else:
		return ".dat"

def srv_get_fname_dir(file_type):
	if (file_type == "vid.cover"):
		return g_dir_httpsrv_pub + "/images"
	elif (file_type == "vid.file"):
		return g_dir_httpsrv_pub + "/videos"
	else:
		return g_dir_httpsrv_pub + "/misc"

def srv_get_fname_name(file_type, file_md5):
	return file_md5[0:3] + "/" + file_md5 + srv_get_fname_ext(file_type)

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

if __name__ == "__main__":
	if os.environ.get("WERKZEUG_RUN_MAIN") is None:
		fork_http_up()
	app.run(host='0.0.0.0', port=9090, debug=True)
	if pid_http_up > 0:
		print("kill http upload service process(pid=%d) ..." % pid_http_up)
		os.kill(pid_http_up, signal.SIGTERM)
		os.waitpid(pid_http_up, 0)

