# -*- coding: iso-8859-15 -*-
"""distlock FunkLoad test

$Id: $
"""
from time import sleep
import unittest
import random
from funkload.FunkLoadTestCase import FunkLoadTestCase
from webunit.utility import Upload

from ws4py.client.threadedclient import WebSocketClient
from ws4py.websocket import WebSocket

class LockClient(WebSocketClient):

    def send(self, payload, binary=False):
       WebSocket.send(self, payload, binary)
       sleep(2)

    def opened(self):
       user = "user-%d" % self.thread_id
       key1 = "lock-%d" % random.randint(0, 10)
       key2 = "lock-%d" % random.randint(0, 10)
       self.send('{ "action": "connect", "param": "' + user + '" }')
       self.send('{ "action": "lock", "param": "' + key1 +'" }')
       self.send('{ "action": "unlock", "param": "' + key1 + '" }')
       self.send('{ "action": "lock", "param": "' + key2 +'" }')
       self.send('{ "action": "unlock", "param": "' + key2 + '" }')
       self.send('{ "action": "close", "param": "' + user + '" }')

    # def closed(self, code, reason):

    def received_message(self, m):
       if 'Close session: ' in str(m):
          self.close(reason='Bye bye')
          self.loop = False


class Distlock(FunkLoadTestCase):
    """XXX

    This test use a configuration file Distlock.conf.
    """

    def setUp(self):
        """Setting up test."""
        self.logd("setUp")
        self.server_url = self.conf_get('main', 'url')
        # XXX here you can setup the credential access like this
        # credential_host = self.conf_get('credential', 'host')
        # credential_port = self.conf_getInt('credential', 'port')
        # self.login, self.password = xmlrpc_get_credential(credential_host,
        #                                                   credential_port,
        # XXX replace with a valid group
        #                                                   'members')

    def test_distlock(self):
        # The description should be set in the configuration file
        server_url = self.server_url
        # begin of test ---------------------------------------------

        # /tmp/tmpVEEir5_funkload/watch0002.request
        ws = LockClient(server_url + '/distlock/lock?X-Atmosphere-tracking-id=0&X-Atmosphere-Framework=1.0&X-Atmosphere-Transport=websocket&X-Cache-Date=0&Content-Type=application/json')
        ws.thread_id = self.thread_id
        ws.loop = True
        ws.daemon = False
        ws.connect()
        while (ws.loop):
           sleep(1)
        ws.close()
        # end of test -----------------------------------------------

    def tearDown(self):
        """Setting up test."""
        self.logd("tearDown.\n")



if __name__ in ('main', '__main__'):
    unittest.main()
