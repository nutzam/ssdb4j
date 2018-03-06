package org.nutz.ssdb4j.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.nutz.ssdb4j.impl.SocketSSDBStream;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.SSDBStream;

public class SsdbPooledObjectFactory extends BasePooledObjectFactory<SSDBStream> {
    
    protected String host;
    protected int port;
    protected int timeout;
    protected byte[] auth;

    public SsdbPooledObjectFactory(String host, int port, int timeout, byte[] auth) {
        super();
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.auth = auth;
    }

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
}