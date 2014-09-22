package org.nutz.ssdb4j.pool;

import java.io.IOException;

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
		SSDBStream steam = null;
		try {
			steam = pool.borrowObject();
			Response resp = steam.req(cmd, vals);
			pool.returnObject(steam);
			return resp;
		} catch (Exception e) {
			if (steam != null)
				try {
					pool.invalidateObject(steam);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			throw new SSDBException(e);
		}
	}

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
	
	public void close() throws IOException {
		try {
			pool.close();
		} catch (Exception e) {
			if (e instanceof IOException)
				throw (IOException)e;
			throw new IOException(e);
		}
	}
}
