package org.nuxeo.ecm.platform.distlock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import redis.clients.jedis.exceptions.JedisException;

/**
 * Singleton to initialize JedisPool and subscriber thread.
 *
 */
public class DistLock {

    public static final String UNLOCKED_CHANNEL = "/distlock/unlock/";

    public static final int EXPIRES_LOCK = 120;

    public static final String MSG_OK = "OK";

    public static final String MSG_FAIL = "FAIL";

    public static final String MSG_RETRY = "RETRY";

    public static final String MSG_WAIT = "WAIT";

    public static final String ACTION_CONNECT = "connect";

    public static final String ACTION_CLOSE = "close";

    public static final String ACTION_LOCK = "lock";

    public static final String ACTION_UNLOCK = "unlock";

    public static final String ACTION_MLOCK = "mlock";

    public static final String ACTION_MUNLOCK = "munlock";

    public static final String WAIT_PREFFIX = "w-";

    public static final String LOCK_PREFFIX = "l-";

    public static final Object NIL = "nil";

    private static final Log log = LogFactory.getLog(DistLock.class);

    private static JedisPool pool;

    private static Jedis subscriberJedis;

    private static Thread subscriberThread;

    private DistLock() {
    }

    public static synchronized JedisPool getJedisPool() {
        if (pool == null) {
            log.info("Initialize DistLock Jedis pool and subscriber thread.");
            pool = new JedisPool(new JedisPoolConfig(), "localhost");
            subscriberThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    subscriberJedis = new Jedis("localhost");
                    try {
                        subscriberJedis.subscribe(new DistLockPubSub(pool),
                                UNLOCKED_CHANNEL);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });
            subscriberThread.start();
        }
        return pool;
    }

    public static synchronized void shutdown() {
        if (pool != null) {
            try {
                pool.destroy();
                pool = null;
            } catch (JedisException e) {
                log.error("Failed to destory jedis pool", e);
            }
        }
        if (subscriberThread != null) {
            subscriberThread.destroy();
            subscriberThread = null;
        }
        if (subscriberJedis != null) {
            try {
                subscriberJedis.disconnect();
                subscriberJedis = null;
            } catch (JedisException e) {
                log.error("Failed to disconnect subscriber", e);
            }
        }
    }

}
