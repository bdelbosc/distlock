/*!
 * distributed lock
 *
 *
*/

$(function () {
    "use strict";

    var detect = $('#detect');
    var header = $('#header');
    var content = $('#content');
    var input = $('#input');
    var status = $('#status');
    var login = $('#login');
    var connect = $('#connect');
    var unlock = $('#unlock');
    var lock = $('#lock');
    var minput = $('#minput');
    var munlock = $('#munlock');
    var mlock = $('#mlock');

    var myName = "anonymous";
    var logged = false;
    var socket = $.atmosphere;

    var rsep = new RegExp("[ ,;]+", "g");

    // We are now ready to cut the request
    var request = { url: document.location.toString() + 'lock',
		    contentType : "application/json",
		    logLevel : 'trace',
		    transport : 'websocket',
		    fallbackTransport: 'long-polling'
		  };

    request.onOpen = function(response) {
        content.html($('<p>', { text: 'Atmosphere connected using ' + response.transport }));
        login.focus();
        status.text('Choose name:');
    };

    <!-- For demonstration of how you can customize the fallbackTransport based on the browser -->
    request.onTransportFailure = function(errorMsg, request) {
        jQuery.atmosphere.info(errorMsg);
        header.html($('<h3>', { text: 'WebSocket Protocol not supported'}));
    };

    request.onReconnect = function (request, response) {
        socket.info("Reconnecting")
    };

    request.onMessage = function (response) {
        var message = response.responseBody;
        try {
            var json = jQuery.parseJSON(message);
        } catch (e) {
            console.log('This doesn\'t look like a valid JSON: ', message.data);
            return;
        }
        input.removeAttr('disabled');
        var date = typeof(json.time) == 'string' ? parseInt(json.time) : json.time;
        addMessage(new Date(date), json.status + ": " + json.message, 'red');
	login.focus();
    };

    request.onClose = function(response) {
	addMessage(new Date(), 'closing', 'blue');
        logged = false;
    }

    request.onError = function(response) {
        content.html($('<p>', { text: 'Sorry, but there\'s some problem with your '
            + 'socket or the server is down' }));
    };

    var subSocket = socket.subscribe(request);

    connect.click(function() {
	if (logged) {
	    // logout
   	    addMessage(new Date(), "Logout " + myName, 'blue');
	    var params = [myName];
            subSocket.push(jQuery.stringifyJSON({ action: 'close', params: params }));
	    login.val('').focus();
	    myName = 'anonymous';
            status.text('Choose name:');
	    connect.val('Connect');
	    logged = false;
	    return false;
	} 
	// login
	var msg = login.val()
        if (msg == '') {
            alert("Please enter a login name");
            return false;
        }
        myName = msg;
	logged = true;
	addMessage(new Date(), "Auth " + myName, 'blue');
	var params = [myName];
        subSocket.push(jQuery.stringifyJSON({ action: 'connect', params: params }));
	status.text(myName + ': ').css('color', 'blue');
	connect.val('Close');
	return false;
    });

    lock.click(function() {
        var msg = input.val();
        if (msg == '') {
            alert("Please enter a lock name");
            return;
        }
	var params = [msg];
	addMessage(new Date(), "TRY LOCK " + msg, 'blue');
        subSocket.push(jQuery.stringifyJSON({ action: 'lock', params: params }));
    });

    unlock.click(function() {
        var msg = input.val();
        if (msg == '') {
            alert("Please enter a lock name");
            return;
        }
	var params = [msg];
	addMessage(new Date(), "TRY UNLOCK " + msg, 'blue');
        subSocket.push(jQuery.stringifyJSON({ action: 'unlock', params: params }));
    });

    mlock.click(function() {
        var msg = minput.val();
        if (msg == '') {
            alert("Please enter a lock name");
            return;
        }
	var params = msg.split(rsep);
	addMessage(new Date(), "TRY MLOCK " + msg, 'blue');
        subSocket.push(jQuery.stringifyJSON({ action: 'mlock', params: params }));
    });

    munlock.click(function() {
        var msg = minput.val();
        if (msg == '') {
            alert("Please enter a lock name");
            return;
        }
	var params = msg.split(rsep);
	addMessage(new Date(), "TRY MUNLOCK " + msg, 'blue');
        subSocket.push(jQuery.stringifyJSON({ action: 'munlock', params: params }));
    });


    function addMessage(datetime, message, color) {
        content.prepend('<p><span style="color: ' + color + '">' + 
            + (datetime.getHours() < 10 ? '0' + datetime.getHours() : datetime.getHours()) + ':'
            + (datetime.getMinutes() < 10 ? '0' + datetime.getMinutes() : datetime.getMinutes()) + ':'
            + (datetime.getSeconds() < 10 ? '0' + datetime.getSeconds() : datetime.getSeconds()) + ': '
            + message + '</span></p>');
    }
});

