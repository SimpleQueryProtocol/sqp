
templateMsgs = [
    {
        type : "H",
        payload : {
           "database":"proxytest"
        }
    },
    {
        type : "S",
        payload : {
            "query":"SELECT * FROM weather"
        }
    },
    {
        type : "S",
        payload : {
            "query":"SELECT * FROM weather",
            "maxFetch":1,
            "cursorId":"S1"
        }
    },
    {
        type : "S",
        payload : {
            "query":"INSERT INTO weather (city, temp_lo, temp_hi, prob, date)\n VALUES ('Tübingen', -13, 34, 1.987654321, '2014-02-02')"
        }
    },
    {
        type : "F",
        payload : {
           "cursorId":"C1",
           "forward":"true",
           "position":-1,
           "maxFetch":2
        }
    },
    {
        type : "L",
        payload : {
            "statements": ["S1"],
            "cursors": ["C1"]
        }
    },
    {
        type : "I",
        payload : {
            "subject":"TypeSchema",
            "detail":"Date"
        }
    },
    {
        type : "P",
        payload : {
           "query": "INSERT INTO weather (city, temp_lo, temp_hi, prob, date)\n VALUES (?, ?, ?, ?, ?)",
            "id": "S2"
        }
    },
    {
        type : "X",
        payload : {
            "statementId": "S2",
            "parameterTypes": ["VarChar", "Integer", "Integer", "Real", "Date"],
            "parameters": [
                ["FooCity", -5, 13, 0.5678, [2015, 3, 12]],
                ["BarCity", 0, 9, 1111111111.0, [1011, 12, 30]],
            ],
        }
    },
    {
        type : "P",
        payload : {
            "query": "SELECT * FROM weather",
            "id": "S1"
        }
    },
    {
        type : "X",
        payload : {
            "statementId": "S1",
            "cursorId": "C1",
            "parameterTypes": [],
            "parameters": [],
            "scrollable": false,
        }
    },
    {
        type : "T",
        payload : {
            "autoCommit": false
        }
    },
]
