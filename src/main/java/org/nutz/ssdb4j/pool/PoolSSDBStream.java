package org.nutz.ssdb4j.pool;

import org.apache.commons.pool.ObjectPool;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class PoolSSDBStream implements SSDBStream {
	
	protected ObjectPool<SSDBStream> pool;
	
	public PoolSSDBStream(ObjectPool<SSDBStream> pool) {
		this.pool = pool;
	}

	public Response req(Cmd cmd, byte[]... vals) {
		try {
			SSDBStream steam = pool.borrowObject();
			try {
				return steam.req(cmd, vals);
			} catch (Exception e) {
				try {
					steam.close();
				} catch (Throwable e2) {
				}
				steam = null;
				throw e;
			}finally {
				if (steam != null)
					pool.returnObject(steam);
			}
		} catch (Exception e) {
			throw new SSDBException(e);
		}
	}

	@Override
	public void callback(SSDBStreamCallback callback) {
		try {
			SSDBStream steam = pool.borrowObject();
			try {
				steam.callback(callback);
			} finally {
				pool.returnObject(steam);
			}
		} catch (Exception e) {
			throw new SSDBException(e);
		}
	}

	@Override
	public void close() throws Exception {
	}
}
