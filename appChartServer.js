const fs = require('fs'); 

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const hostname = '127.0.0.1';
const port = 3000;

const appExpress = express();
const server = http.createServer(appExpress);
const wss = new WebSocket.Server({ server, });

//appExpress.set('view engine', 'pug');

appExpress.get('/js/:filename', (req, res) => {
    var filename = req.params.filename;
    var filepath = __dirname + '/code/js';
    try {
	  fs.accessSync(filepath + '/' + filename, fs.constants.F_OK);
    }
    catch (e) {
      filepath = __dirname + '/static/js';
    }

    var options = {
      root: filepath,
      dotfiles: 'deny',
      headers: {
        'x-timestamp': Date.now(),
        'x-sent': true,
      },
    };

    res.set('Content-Type', 'application/javascript');
    res.sendFile(filename, options, function (err) {
        console.log('Sent:', filepath + '/' + filename);
      });
    });

appExpress.get('/css/:filename', (req, res) => {
    var filename = req.params.filename;
    var filepath = __dirname + '/static/css';

    var options = {
      root: filepath,
      dotfiles: 'deny',
      headers: {
        'x-timestamp': Date.now(),
        'x-sent': true,
      },
    };

    res.set('Content-Type', 'text/css');
    res.sendFile(filename, options, function (err) {
        console.log('Sent:', filepath + '/' + filename);
      });
    });

appExpress.get('/demo/:filename', (req, res) => {
    var filename = req.params.filename;
    var filepath = __dirname + '/demo/html';

    var options = {
      root: filepath,
      dotfiles: 'deny',
      headers: {
        'x-timestamp': Date.now(),
        'x-sent': true,
      },
    };

    res.set('Content-Type', 'text/html');
    res.sendFile(filename, options, function (err) {
        console.log('Sent:', filepath + '/' + filename);
      });
    });

appExpress.get('/views/:filename', (req, res) => {
    var filename = req.params.filename;
    var filepath = __dirname + '/views';

    var options = {
      root: filepath,
      dotfiles: 'deny',
      headers: {
        'x-timestamp': Date.now(),
        'x-sent': true,
      },
    };

    res.set('Content-Type', 'text/html');
    res.sendFile(filename, options, function (err) {
        console.log('Sent:', filepath + '/' + filename);
      });
    });

appExpress.get('/', (req, res) => {
      res.statusCode = 200;
      res.setHeader('Content-Type', 'text/plain');
      res.end('Hello World\n');
    });

/*
@app.route("/")
@app.route("/index.html")
def def_static_index():
	return send_from_directory(app.static_folder + '/html', 'index.html')
// */

wss.on('connection', (ws, req) => {
    //connection is up, let's add a simple simple event
    ws.on('message', (message) => {
        //log the received message and send it back to the client
        console.log('received: %s', message);
        ws.send(`Hello, you sent -> ${message}`);
    });

    //send immediatly a feedback to the incoming connection
    ws.send('Hi there, I am a WebSocket server');
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});


