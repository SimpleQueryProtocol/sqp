// Some minimal Javascript examples

// Simple query
query = "INSERT INTO weather (city, temp_lo, temp_hi, date) " +
        "VALUES ('Stuttgart', 13, 28, '2015-08-21')";
websocket = new WebSocket("ws://localhost:8080/");
websocket.onopen = function(e) {
    websocket.send('H' + JSON.stringify({database : "proxytest"}));
    websocket.send('S' + JSON.stringify({query: query}));
    websocket.close();
};

// With databind

now = new Date();
date = [now.getFullYear(), now.getMonth() + 1, now.getDate()];
query = "INSERT INTO weather (city, temp_lo, temp_hi, date) VALUES (?,?,?,?)";
websocket = new WebSocket("ws://localhost:8080/");
websocket.onopen = function(e) {
    websocket.send('H' + JSON.stringify({database : "proxytest"}));
    websocket.send('P' + JSON.stringify({query: query}));
    websocket.send('X' + JSON.stringify({
       parameterTypes: ['VarChar', 'Integer', 'Integer', 'Date'],
       parameters: [['Stuttgart', 13, 28, date]]
    }))
    websocket.close();
};
