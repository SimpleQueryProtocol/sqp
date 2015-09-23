var websocket;
var ping;
var msgarea;

function usetpl(id) {
    msgarea.val(templateMsgs[id]["type"] + " " + JSON.stringify(templateMsgs[id]["payload"], null, 4));
}

function jsonpp(obj) {
  return JSON.stringify(obj, null, '\t').replace(/\n/g, "<br>").replace(/\t/g, "&nbsp;&nbsp;&nbsp;&nbsp;");;
}

jQuery(function ($) {
  msgarea = $('#message');
  $('#connect').click(doConnect);
  $('#disconnect').click(doDisconnect);
  $('#send').click(function () {
    doSend( msgarea.val() )
  });
  $('#clearlog').click(doClearLog);
  $.getScript('template_msgs.js', function() {
      var tpllist = $('#msgtemplates');
      templateMsgs.forEach(function(element, idx) {
        tpllist.append(
           '<li>' +
             '<button onclick="usetpl(' + idx + ')">Use</button>' +
             '<div>\n' + element["type"] + jsonpp(element["payload"]) + '</div>' +
           '</li>'
        );
      })
  });

  function doConnect() {
    websocket = new WebSocket( $("#url").val() );
    websocket.onopen = function (evt) {
        onOpen(evt)
    };
    websocket.onclose = function (evt) {
        onClose(evt)
    };
    websocket.onmessage = function (evt) {
        onMessage(evt)
    };
    websocket.onerror = function (evt) {
        onError(evt)
    };
  }

  function doClearLog() {
    $('#log').empty();
  }

  function doDisconnect() {
    websocket.close();
  }

  function onOpen(evt) {
    writeToScreen("CONNECTED", "green");
  }

  function onClose(evt) {
    writeToScreen("DISCONNECTED", "red");
  }

  function onMessage(evt) {
    var data = evt.data;
    var code = data[0];
    var contents = "";
    if (data.length > 1) {
        contents = jsonpp(JSON.parse(data.substring(1)));
    }
    writeToScreen('RECEIVED: ' + code + '<br />' + contents);
  }

  function onError(evt) {
    writeToScreen('ERROR:' + evt.data, "red");
  }

  function doSend(message) {
    writeToScreen('SENT: ' + message.replace(/^ +/gm, '&nbsp;&nbsp;&nbsp;&nbsp;').replace(/\n/g, "<br>"), "blue");
    websocket.send(message);
  }

  function writeToScreen(message, className) {
    var clazz = typeof className !== 'undefined' ? (" class='" + className + "'") : "";
    var logDiv = $('#log');
    logDiv.append('<p' + clazz + '>' + message + '</p>');
    logDiv.scrollTop(logDiv[0].scrollHeight);
  }
});
