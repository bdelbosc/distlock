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
import org.atmosphere.cpr.AtmosphereResource;
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
 * {@link WebSocketHandler} that implement the logic to build a
 * Distributed lock
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
        Jedis jedis = pool.getResource();
        try {
            processAction(webSocket, req, jedis);
        } finally {
            pool.returnResource(jedis);
        }
    }

    private void processAction(WebSocket webSocket, Request req, Jedis jedis) {

        AtmosphereResource r = webSocket.resource();
        String action = req.action;
        String cid = r.uuid(); // connection id
        String sid = jedis.get(cid); // user session id
        if (DistLock.NIL.equals(sid)) {
            sid = null;
        }
        if (DistLock.ACTION_LOCK.equals(req.action)) {
            if (sid == null) {
                r.getResponse().write(
                        new Response(DistLock.MSG_FAIL,
                                "Not sid, connect first").toString());
                return;
            }
            String lockname = req.param;
            log.debug("Try lock: " + lockname);
            String key = DistLock.LOCK_PREFFIX + lockname;
            if (jedis.setnx(key, sid) == 1) {
                // Acquire lock
                jedis.expire(key, DistLock.EXPIRES_LOCK);
                r.getResponse().write(
                        new Response(DistLock.MSG_OK, "Acquired").toString());
                log.debug("Lock set on " + key + " by " + sid + "/" + cid);
                return;
            }
            String owner = jedis.get(key);
            if (sid.equals(owner)) {
                // Already acquire
                jedis.expire(key, DistLock.EXPIRES_LOCK);
                log.debug("Already got the lock on " + key);
                r.getResponse().write(
                        new Response(DistLock.MSG_OK, "Already own the lock").toString());
                return;
            }
            if (jedis.ttl(key) == -1) {
                // paranoid check
                jedis.expire(key, DistLock.EXPIRES_LOCK);
            }
            // lock wait
            key = DistLock.WAIT_PREFFIX + lockname;
            jedis.sadd(key, sid);
            r.getResponse().write(
                    new Response(DistLock.MSG_WAIT, "Lock owned by " + owner).toString());
            log.debug("Lock " + key + " owned by " + owner + ", " + sid
                    + " waiting.");
        } else if (DistLock.ACTION_UNLOCK.equals(action)) {
            if (sid == null) {
                r.getResponse().write(
                        new Response(DistLock.MSG_FAIL,
                                "Not sid, connect first").toString());
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
                    r.getResponse().write(
                            new Response(DistLock.MSG_FAIL,
                                    "Transaction failed during unlock: "
                                            + e.getMessage()).toString());
                    return;
                }
                log.debug("Unlock " + lockname + " done");
            } else {
                log.debug("lock owned by " + owner + " not " + sid);
                r.getResponse().write(
                        new Response(DistLock.MSG_FAIL, "lock owned by "
                                + owner).toString());
                jedis.unwatch();
                return;
            }
            jedis.unwatch();

            r.getResponse().write(
                    new Response(DistLock.MSG_OK, "Unlocked " + lockname).toString());

            jedis.publish(DistLock.UNLOCKED_CHANNEL, lockname);

            // TODO: code to move to the subscriber

        } else if (DistLock.ACTION_CONNECT.equals(req.action)) {
            sid = req.param;
            jedis.set(sid, cid);
            jedis.set(cid, sid);
            r.getResponse().write(
                    new Response(DistLock.MSG_OK, "Open session: " + sid).toString());
        } else if (DistLock.ACTION_CLOSE.equals(req.action)) {
            sid = req.param;
            jedis.del(sid);
            jedis.del(cid);
            r.getResponse().write(
                    new Response(DistLock.MSG_OK, "Close session: " + sid).toString());
            // TODO: remove all lock with value sessionid
        } else {
            log.error("Unknown action: " + action);
        }

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
