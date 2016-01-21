var http = require('http');
var sockjs = require('sockjs');
var node_static = require('node-static');
var zmq = require('zmq');

var pipe = zmq.socket('pair');
pipe.connect('tcp://127.0.0.1:5004');
pipe.send(['RESET']);

var lastHugs = Date.now();
function reset() {
    console.log('reseting connections');
    for (var i in connections) {
        connections[i].end();
    }
    connections = {}
}
setInterval(function () {
    if (Date.now() - lastHugs > 500) {
        reset();
        lastHugs = Date.now();
    }
}, 2000);
pipe.on('message', function (clientId, path, seq, props, value) {
    lastHugs = Date.now();


    if (arguments.length === 1) {
        if (clientId === 'RESET') {
            reset();
        }
    }
    if (arguments.length === 5) {
        //console.log(arguments);
        //console.log(clientId.toString(), path.toString(), seq.readUInt8(), cmdId.toString(), props.toString(), value.toString());

        var ids = clientId.toString().split(/\|/);
        //console.log(ids);
        for (var i in ids) {
            var id = ids[i];
            var conn = connections[id];
            if (conn) {
                conn.write([
                    path.toString(),
                    seq.length > 0 ? seq.readUInt8() : null,
                    props.length > 0 ? props.toString() : null,
                    value.length > 0 ? value.toString() : null]);
            } else {
                pipe.send(['UNSUB', id]);
            }
        }
    }
});

// 1. Echo sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.jsdelivr.net/sockjs/1.0.1/sockjs.min.js"};

var sockjs_echo = sockjs.createServer(sockjs_opts);
var lastId = 0;
var connections = {};
var cmd = {
    sub: 'SUB',
    unsub: 'UNSUB'
};
sockjs_echo.on('connection', function (conn) {
    var id = "" + (++lastId);
    connections[id] = conn;
    console.log('Connected ', id);
    conn.on('data', function (message) {
        var match = message.match(/(sub|unsub) ([\/\w_-]+)/);
        if (match) {
            pipe.send([cmd[match[1]], id, match[2]]);
            conn.write("OK");
        } else {
            conn.write("ERR");
        }
    });
    conn.on('close', function () {
        console.log('Disconnected', id);
        pipe.send(['UNSUB', id]);
        delete connections[id];
    });
});

// 2. Static files server
var static_directory = new node_static.Server(__dirname);

// 3. Usual http stuff
var server = http.createServer();
server.addListener('request', function (req, res) {
    static_directory.serve(req, res);
});
server.addListener('upgrade', function (req, res) {
    res.end();
});

sockjs_echo.installHandlers(server, {prefix: '/echo'});

console.log(' [*] Listening on 0.0.0.0:9999');
server.listen(9999, '0.0.0.0');