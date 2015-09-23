/*
  This example is the JavaScript equivalent to DatabaseIndependentExample.java
*/
// some needed definitions
months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
MONTH_DAY_SCHEMA = {
    type: "array",
    minItems: 2,
    additionalItems: false,
    items: [
        {type: "integer", "minimum": 1, "maximum": 12},
        {type: "integer", "minimum": 1, "maximum": 31}
    ]
};

// the main function with the actual logic
function main() {
    send('S', {query: "DELETE FROM friends"});
    send('I', {subject: 'DBMSName'}, {
        end : 'i',
        i: function(msg) { console.log("DBMS: " + msg.value)}
    });
    send('M', {name: "monthDay", schema: MONTH_DAY_SCHEMA,
               keywords: ["point", "[mo:dd]"]},{
        end: 'm',
        m: function (msg) { console.log("Mapping to " + msg.native) }
    });

    var names = [ "John Doe", "Mary Jane" ];
    var birthdays = [[2, 29], [12, 31]];
    var heights = [1.84, 1.59];
    console.log("Data to insert: ");
    for (var i = 0; i < names.length; i++) {
        printPerson(names[i], birthdays[i], heights[i]);
    }

    var insertStmt = "INSERT INTO friends " +
                     "(name, birthday, height) VALUES (?, ?, ?)";
    send('P', {query : insertStmt});
    send('X', {
           parameterTypes: ['VarChar', 'Custom', 'Real'],
           customTypes: ["monthDay"],
           parameters: [
                [names[0], birthdays[0], heights[0]],
                [names[1], birthdays[1], heights[1]],
           ]},{
        end: 'x',
        x: function(msg) {
            console.log("Inserted " + msg.affectedRows + " friends.")
    }});

    var handler = { end: 'e' };
    handler['c'] = function(msg) { console.log("Actual data: ")};
    handler['#'] = function(msg) {
        printPerson(msg.data[0], msg.data[1], msg.data[2])
    };
    var selectStmt = "SELECT name, birthday, height FROM friends";
    send('S', { query : selectStmt }, handler);
}

// Other utility
function printPerson(name, birthday, height) {
  console.log(name + " is " + height + "m tall and has birthday on " +
              months[birthday[0]] + ", " + birthday[1]);
}

// Functions to handle and send SQP messages
databaseName = "exampleDB";
mainmethod = main;
handlers = [];
function messageHandler(evt) {
    var msg = evt.data;
    var id = msg[0];
    var payload = msg.length < 2 ? null : JSON.parse(msg.substring(1));
    if (id == '!') {
        console.log("Error " + payload.errorType + ": " + payload.message);
        return;
    }
    if (handlers.length < 1) {
       return;
    }
    if (id in handlers[0]) {
        handlers[0][id](payload);
    }
    if (handlers[0].end == id) {
       handlers.shift();
    }
}
function send(id, content, handler) {
   if (handler) handlers.push(handler);
   websocket.send(id + JSON.stringify(content))
}

function start() {
    send('H', {database : databaseName}, {
        end: 'r',
        r: mainmethod
    });
}

// This code actually invokes the processing
websocket = new WebSocket("ws://localhost:8080/");
websocket.onopen = start;
websocket.onmessage = messageHandler;
