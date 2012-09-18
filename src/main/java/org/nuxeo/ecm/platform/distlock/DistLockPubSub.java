package org.nuxeo.ecm.platform.distlock;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Jedis subscriber, listen to unlock message and forward notification to
 * browsers that are waiting for the lock.
 *
 * @author ben
 *
 */
public class DistLockPubSub extends JedisPubSub {

    private static final Log log = LogFactory.getLog(DistLockPubSub.class);

    private JedisPool pool;

    public DistLockPubSub(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public void onMessage(String channel, String message) {
        log.debug("onMessage: " + channel + ": " + message);
        if (DistLock.UNLOCKED_CHANNEL.equals(channel)) {
            String lockname = message;
            String key = DistLock.WAIT_PREFFIX + lockname;
            Jedis jedis = pool.getResource();
            try {
                Set<String> items = jedis.smembers(key);
                for (String item : items) {
                    String uid = jedis.get(item);
                    AtmosphereResource find = AtmosphereResourceFactory.getDefault().find(
                            uid);
                    if (find != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Nofify retry lock: " + lockname
                                    + " to: " + item);
                        }
                        find.getResponse().write(
                                new WebSocketLock.Response(DistLock.MSG_RETRY,
                                        lockname).toString());
                        jedis.srem(key, item);
                    } else {
                        log.debug("Not found ressource for retry lock " + item);
                    }
                }
            } finally {
                pool.returnResource(jedis);
            }
        }
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        log.debug("onSubscribe: " + channel);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        log.debug("onUnsubscribe: " + channel);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        log.debug("onPMessage: " + pattern + "/" + channel + ": " + message);
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        log.debug("onPSubscribe: " + pattern);
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        log.debug("onPUnsubscribe: " + pattern);
    }

}
