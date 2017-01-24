package org.nutz.ssdb4j.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.impl.SocketSSDBStream;
import org.nutz.ssdb4j.spi.Cmd;
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
        return new PoolSSDBStream(new GenericObjectPool<SSDBStream>(new BasePooledObjectFactory<SSDBStream>() {

            public SSDBStream create() throws Exception {
                return new SocketSSDBStream(host, port, timeout, auth);
            }

            public PooledObject<SSDBStream> wrap(SSDBStream obj) {
                return new DefaultPooledObject<SSDBStream>(obj);
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
        }, (GenericObjectPoolConfig)cnf));
    }
}
