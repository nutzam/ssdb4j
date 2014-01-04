package org.nutz.ssdb.pool;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.nutz.ssdb.spi.SSDBStream;

public class SocketSSDBStreamPool extends GenericObjectPool<SSDBStream> {

	public SocketSSDBStreamPool(String host, int port, int timeout, Config config) {
		super(new SocketSSDBStreamFactory(host, port, timeout), config);
	}

}
