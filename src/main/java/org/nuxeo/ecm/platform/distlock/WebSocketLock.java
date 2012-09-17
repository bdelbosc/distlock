/*
 * Copyright 2012 Jeanfrancois Arcand
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.nuxeo.ecm.platform.distlock;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atmosphere.config.service.WebSocketHandlerService;
import org.atmosphere.cpr.AtmosphereResponse;
import org.atmosphere.websocket.WebSocket;
import org.atmosphere.websocket.WebSocketHandler;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

/**
 * {@link WebSocketHandler} that implement the logic to build a Distributed lock
 *
 */
@WebSocketHandlerService
public class WebSocketLock extends WebSocketHandler {

    private static final Log log = LogFactory.getLog(WebSocketLock.class);

    private JedisPool pool;

    @Override
    public void onOpen(WebSocket webSocket) {
        log.debug("Opening new websocket/jedis");

        webSocket.resource().suspend(-1);
        pool = DistLock.getJedisPool();
        log.debug("Opened " + webSocket.resource().uuid() + " get jedis conn");
    }

    @Override
    public void onClose(WebSocket webSocket) {
        log.debug("Closing websocket " + webSocket.resource().uuid());
        super.onClose(webSocket);
        log.debug("Closed");
    }

    public void onTextMessage(WebSocket webSocket, String message) {
        log.debug("onTextMessage " + webSocket.resource().uuid() + " "
                + message);
        ObjectMapper mapper = new ObjectMapper();
        Request req = null;
        try {
            req = mapper.readValue(message, Request.class);
        } catch (JsonParseException e) {
            log.error(e.getMessage(), e);
            return;

        } catch (JsonMappingException e) {
            log.error(e.getMessage(), e);
            return;

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return;
        }
        String action = req.action;
        Jedis jedis = pool.getResource();
        try {
            if (DistLock.ACTION_LOCK.equals(action)) {
                lock(webSocket, req, jedis);
            } else if (DistLock.ACTION_UNLOCK.equals(action)) {
                unlock(webSocket, req, jedis);
            } else if (DistLock.ACTION_CONNECT.equals(req.action)) {
                connect(webSocket, req, jedis);
            } else if (DistLock.ACTION_CLOSE.equals(req.action)) {
                close(webSocket, req, jedis);
            } else {
                log.error("Unknown action: " + action);
            }
        } finally {
            pool.returnResource(jedis);
        }
    }

    private void close(WebSocket webSocket, Request req, Jedis jedis) {
        String sid = req.param;
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        jedis.del(sid);
        jedis.del(cid);
        resp.write(new Response(DistLock.MSG_OK, "Close session: " + sid).toString());
        // TODO: remove all lock with value sessionid
    }

    private void connect(WebSocket webSocket, Request req, Jedis jedis) {
        String sid = req.param;
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        jedis.set(sid, cid);
        jedis.set(cid, sid);
        resp.write(new Response(DistLock.MSG_OK, "Open session: " + sid).toString());
    }

    private void unlock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return;
        }
        String lockname = req.param;
        log.debug("Try Unlock" + lockname);
        String key = DistLock.LOCK_PREFFIX + lockname;
        jedis.watch(key);
        String owner = jedis.get(key);
        if (sid.equals(owner)) {
            Transaction t = jedis.multi();
            t.del(key);
            try {
                t.exec();
            } catch (JedisException e) {
                log.error("Failed to unlock " + lockname, e);
                resp.write(new Response(DistLock.MSG_FAIL,
                        "Transaction failed during unlock: " + e.getMessage()).toString());
                return;
            }
            log.debug("Unlock " + lockname + " done");
        } else {
            log.debug("lock owned by " + owner + " not " + sid);
            resp.write(new Response(DistLock.MSG_FAIL, "lock owned by " + owner).toString());
            jedis.unwatch();
            return;
        }
        jedis.unwatch();

        resp.write(new Response(DistLock.MSG_OK, "Unlocked " + lockname).toString());
        jedis.publish(DistLock.UNLOCKED_CHANNEL, lockname);

        // TODO: code to move to the subscriber
    }

    private boolean lock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return false;
        }
        String lockname = req.param;
        log.debug("Try lock: " + lockname);
        String key = DistLock.LOCK_PREFFIX + lockname;
        if (jedis.setnx(key, sid) == 1) {
            // Acquire lock
            jedis.expire(key, DistLock.EXPIRES_LOCK);
            resp.write(new Response(DistLock.MSG_OK, "Acquired").toString());
            log.debug("Lock set on " + key + " by " + sid + "/" + cid);
            return true;
        }
        String owner = jedis.get(key);
        if (sid.equals(owner)) {
            // Already acquire
            jedis.expire(key, DistLock.EXPIRES_LOCK);
            log.debug("Already got the lock on " + key);
            resp.write(new Response(DistLock.MSG_OK, "Already own the lock").toString());
            return true;
        }
        if (jedis.ttl(key) == -1) {
            // paranoid check
            jedis.expire(key, DistLock.EXPIRES_LOCK);
        }
        // lock wait
        key = DistLock.WAIT_PREFFIX + lockname;
        jedis.sadd(key, sid);
        resp.write(new Response(DistLock.MSG_WAIT, "Lock owned by " + owner).toString());
        log.debug("Lock " + key + " owned by " + owner + ", " + sid
                + " waiting.");
        return false;
    }

    private String getSessionId(Jedis jedis, String cid) {
        String sid = jedis.get(cid);
        if (DistLock.NIL.equals(sid)) {
            sid = null;
        }
        return sid;
    }

    private String getConnectionId(Jedis jedis, String sid) {
        String cid = jedis.get(sid);
        if (DistLock.NIL.equals(cid)) {
            cid = null;
        }
        return cid;
    }

    public final static class Request {

        public String action;

        public String param;

        public String toString() {
            return "{ \"action\" : \"" + action + "\", \"param\" : \"" + param
                    + "\" , \"time\" : " + new Date().getTime() + "}";
        }
    }

    public final static class Response {

        public String status;

        public String message;

        public Response(String status, String message) {
            this.status = status;
            this.message = message;
        }

        public String toString() {
            return "{ \"status\" : \"" + status + "\", \"message\" : \""
                    + message + "\", \"time\" : " + new Date().getTime() + "}";
        }

    }

}
