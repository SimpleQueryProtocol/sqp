/*
 --pg
CREATE TABLE people
(
   name character varying(80),
   birthday date,
   height real
);


--tb
CREATE TABLE people
(
   name character varying(80),
   birthday date,
   height float
);
*/
function main() {
    send('S', {query: "DELETE FROM people"});
    var insertStmt = "INSERT INTO people " +
                    "(name, birthday, height) VALUES (?, ?, ?)";
    send('I', {subject: 'DBMSName'}, {
        end : 'i',
        i: function(msg) { console.log("DBMS: " + msg.value)}
    });
    send('P', {query : insertStmt});
    send('X', {
            parameterTypes: ['VarChar', 'Date', 'Real'],
            parameters: [
                ["John Doe",  [1989, 7, 31], 1.82],
                ["Jan Bauer", [1960, 12, 2], 1.48],
            ]},{
        end: 'x',
        x: function(msg) {
            console.log("Inserted " + msg.affectedRows + " people.")
    }});

    var handler = { end: 'e' };
    handler['c'] = function(msg) { console.log("Actual data: ")};
    handler['#'] = function(msg) {
        var bday = msg.data[1][2] + "." + msg.data[1][1] + "." + msg.data[1][0];
        console.log(msg.data[0] + " (" + msg.data[2] +"m) has bday on " + bday);
    };
    var selectStmt = "SELECT name, birthday, height FROM people";
    send('S', { query : selectStmt }, handler);
}

databaseName = "exampleDB";
mainmethod = main;
handlers = [];
function send(id, content, handler) {
   if (handler) handlers.push(handler);
   websocket.send(id + JSON.stringify(content))
}
function messageHandler(evt) {
    var msg = evt.data;
    var id = msg[0];
    var payload = msg.length < 2 ? null : JSON.parse(msg.substring(1));
    if (id == '!') {
        console.log("Error " + payload.errorType + ": " + payload.message);
        return;
    }
    if (handlers.length < 1) return;
    if (id in handlers[0]) handlers[0][id](payload);
    if (handlers[0].end == id) handlers.shift();
}
function start() {
    send('H', {database : databaseName}, { end: 'r', r: mainmethod });
}

websocket = new WebSocket("ws://localhost:8080/");
websocket.onopen = start;
websocket.onmessage = messageHandler;
