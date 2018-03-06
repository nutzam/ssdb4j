package org.nutz.ssdb4j.pool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.spi.SSDBStream;

public class Pools {
    
    public static PoolSSDBStream pool(final String host, final int port, final int timeout, Object cnf) {
        return pool(host, port, timeout, cnf, null);
    }

    public static PoolSSDBStream pool(final String host, final int port, final int timeout, Object cnf, final byte[] auth) {
        if (cnf == null) {
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            config.setMaxTotal(10);
            config.setTestWhileIdle(true);
            cnf = config;
        }
        return new PoolSSDBStream(new GenericObjectPool<SSDBStream>(new SsdbPooledObjectFactory(host, port, timeout, auth), (GenericObjectPoolConfig)cnf));
    }
}
