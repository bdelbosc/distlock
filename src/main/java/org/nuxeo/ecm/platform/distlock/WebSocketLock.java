package org.nuxeo.ecm.platform.distlock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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

    @Override
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
            } else if (DistLock.ACTION_MLOCK.equals(action)) {
                mlock(webSocket, req, jedis);
            } else if (DistLock.ACTION_MUNLOCK.equals(action)) {
                munlock(webSocket, req, jedis);
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

    // -----------------------------------------------------------
    // DistLock API
    private void connect(WebSocket webSocket, Request req, Jedis jedis) {
        String sid = req.getParam();
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        jedis.set(sid, cid);
        jedis.set(cid, sid);
        resp.write(new Response(DistLock.MSG_OK, "Open session: " + sid).toString());
        if (log.isDebugEnabled()) {
            log.debug("Session opened: " + sid + "/" + cid);
        }
    }

    private void close(WebSocket webSocket, Request req, Jedis jedis) {
        String sid = req.getParam();
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        jedis.del(sid);
        jedis.del(cid);
        resp.write(new Response(DistLock.MSG_OK, "Close session: " + sid).toString());
        // TODO: remove all lock with value sessionid
        if (log.isDebugEnabled()) {
            log.debug("Sesison closed: " + sid + "/" + cid);
        }
    }

    private boolean lock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return false;
        }
        String lockname = req.getParam();
        if (log.isDebugEnabled()) {
            log.debug(sid + " try locking: " + lockname);
        }
        String key = DistLock.LOCK_PREFFIX + lockname;
        if (jedis.setnx(key, sid) == 1) {
            jedis.expire(key, DistLock.EXPIRES_LOCK);
            if (log.isDebugEnabled()) {
                log.debug(sid + " locked: " + lockname);
            }
            resp.write(new Response(DistLock.MSG_OK, "Acquired").toString());
            return true;
        }
        String owner = jedis.get(key);
        if (sid.equals(owner)) {
            // Already acquire
            jedis.expire(key, DistLock.EXPIRES_LOCK);
            if (log.isDebugEnabled()) {
                log.debug(sid + " already locked: " + lockname);
            }
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
        if (log.isDebugEnabled()) {
            log.debug(sid + " waiting for lock: " + lockname + " owned by "
                    + owner);
        }
        resp.write(new Response(DistLock.MSG_WAIT, "Lock owned by " + owner).toString());
        return false;
    }

    private boolean unlock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return false;
        }
        String lockname = req.getParam();
        if (log.isDebugEnabled()) {
            log.debug(sid + " try unlocking: " + lockname);
        }
        String key = DistLock.LOCK_PREFFIX + lockname;
        jedis.watch(key);
        String owner = jedis.get(key);
        if (sid.equals(owner)) {
            Transaction t = jedis.multi();
            t.del(key);
            try {
                t.exec();
            } catch (JedisException e) {
                log.error(sid + " failed to unlock " + lockname, e);
                resp.write(new Response(DistLock.MSG_FAIL,
                        "Transaction failed during unlock: " + e.getMessage()).toString());
                return false;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug(sid + " can not unlock " + lockname + " owned by "
                        + owner);
            }
            resp.write(new Response(DistLock.MSG_FAIL, "lock owned by " + owner).toString());
            jedis.unwatch();
            return false;
        }
        jedis.unwatch();
        if (log.isDebugEnabled()) {
            log.debug(sid + " unlocked: " + lockname);
        }
        resp.write(new Response(DistLock.MSG_OK, "Unlocked " + lockname).toString());
        jedis.publish(DistLock.UNLOCKED_CHANNEL, lockname);
        return true;
    }

    private boolean mlock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return false;
        }
        String[] locknames = req.getParams();
        if (log.isDebugEnabled()) {
            log.debug(sid + " try mlocking: " + Arrays.toString(locknames));
        }
        String[] keysvalues = new String[2 * locknames.length];
        String[] keys = new String[locknames.length];
        for (int i = 0; i < locknames.length; i++) {
            keysvalues[2 * i] = DistLock.LOCK_PREFFIX + locknames[i];
            keysvalues[2 * i + 1] = sid;
            keys[i] = DistLock.LOCK_PREFFIX + locknames[i];
        }
        if (jedis.msetnx(keysvalues) == 1) {
            // TODO pipeline all cmd
            for (String key : keys) {
                jedis.expire(key, DistLock.EXPIRES_LOCK);
            }
            if (log.isDebugEnabled()) {
                log.debug(sid + " mlocked: " + Arrays.toString(locknames));
            }
            resp.write(new Response(DistLock.MSG_OK, "Acquired").toString());
            return true;
        }
        List<String> owners = jedis.mget(keys);
        String owner = null;
        for (String item : owners) {
            if (item != null && !sid.equals(item)) {
                owner = item;
                break;
            }
        }
        if (owner == null) {
            for (String key : keys) {
                jedis.expire(key, DistLock.EXPIRES_LOCK);
            }
            if (log.isDebugEnabled()) {
                log.debug(sid + " already mlocked: "
                        + Arrays.toString(locknames));
            }
            resp.write(new Response(DistLock.MSG_OK, "Already own the lock").toString());
            return true;
        }
        for (String key : keys) {
            // paranoid check
            if (jedis.ttl(key) == -1) {
                jedis.expire(key, DistLock.EXPIRES_LOCK);
            }
        }
        // lock wait
        for (String lockname : locknames) {
            jedis.sadd(DistLock.WAIT_PREFFIX + lockname, sid);
        }
        if (log.isDebugEnabled()) {
            log.debug(sid + " wait for mlock: " + Arrays.toString(locknames)
                    + " some are owned by " + owner);
        }
        resp.write(new Response(DistLock.MSG_WAIT, "Lock owned by " + owner).toString());
        return false;
    }

    private boolean munlock(WebSocket webSocket, Request req, Jedis jedis) {
        String cid = webSocket.resource().uuid(); // connection id
        AtmosphereResponse resp = webSocket.resource().getResponse();
        String sid = getSessionId(jedis, cid); // user session id
        if (sid == null) {
            resp.write(new Response(DistLock.MSG_FAIL, "Not sid, connect first").toString());
            return false;
        }
        String[] locknames = req.getParams();
        if (log.isDebugEnabled()) {
            log.debug(sid + " try munlocking: " + Arrays.toString(locknames));
        }
        String[] keys = new String[locknames.length];
        for (int i = 0; i < locknames.length; i++) {
            keys[i] = DistLock.LOCK_PREFFIX + locknames[i];
        }
        jedis.watch(keys);
        List<String> owners = jedis.mget(keys);
        for (String owner : owners) {
            if (!sid.equals(owner)) {
                if (log.isDebugEnabled()) {
                    log.debug(sid + " can not munlock "
                            + Arrays.toString(locknames)
                            + " some are owned by " + owner);
                }
                resp.write(new Response(DistLock.MSG_FAIL,
                        "Can not munlock, some are owned by " + owner).toString());
                jedis.unwatch();
                return false;
            }
        }
        Transaction t = jedis.multi();
        t.del(keys);
        try {
            t.exec();
        } catch (JedisException e) {
            log.error("Failed to munlock " + Arrays.toString(locknames), e);
            resp.write(new Response(DistLock.MSG_FAIL,
                    "Transaction failed during unlock: " + e.getMessage()).toString());
            return false;
        }
        jedis.unwatch();
        if (log.isDebugEnabled()) {
            log.debug(sid + " munlocked: " + Arrays.toString(locknames));
        }
        resp.write(new Response(DistLock.MSG_OK, "MUnlocked "
                + Arrays.toString(locknames)).toString());
        for (String lockname : locknames) {
            jedis.publish(DistLock.UNLOCKED_CHANNEL, lockname);
        }
        return true;

    }

    // -----------------------------------------------------------
    // Helpers
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

        public String[] params;

        public String getParam() {
            return params[0];
        }

        public String[] getParams() {
            return params;
        }

        public String toString() {
            return "{ \"action\" : \"" + action + "\", \"param\" : \""
                    + getParam() + "\" , \"time\" : " + new Date().getTime()
                    + "}";
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
