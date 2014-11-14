package org.nutz.ssdb4j.pool;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.impl.SocketSSDBStream;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.SSDBStream;

public class Pools {
    
    public static PoolSSDBStream pool(final String host, final int port, final int timeout, Object cnf) {
        return pool(host, port, timeout, cnf, null);
    }

    public static PoolSSDBStream pool(final String host, final int port, final int timeout, Object cnf, final byte[] auth) {
        if (cnf == null) {
            Config config = new Config();
            config.maxActive = 10;
            config.testWhileIdle = true;
            cnf = config;
        }
        return new PoolSSDBStream(new GenericObjectPool<SSDBStream>(new BasePoolableObjectFactory<SSDBStream>() {
            
            public SSDBStream makeObject() throws Exception {
                return new SocketSSDBStream(host, port, timeout, auth);
            }

            public boolean validateObject(SSDBStream stream) {
                try {
                    return stream.req(Cmd.ping).ok();
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            
            public void destroyObject(SSDBStream obj) throws Exception {
                obj.close();
            }
        }, (Config)cnf));
    }
}
