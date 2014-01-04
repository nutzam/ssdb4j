package org.nutz.ssdb.pool;

import org.apache.commons.pool.ObjectPool;
import org.nutz.ssdb.spi.Cmd;
import org.nutz.ssdb.spi.Respose;
import org.nutz.ssdb.spi.SSDBException;
import org.nutz.ssdb.spi.SSDBStream;

public class PoolSSDBStream implements SSDBStream {
	
	protected ObjectPool<SSDBStream> pool;
	
	public PoolSSDBStream(ObjectPool<SSDBStream> pool) {
		this.pool = pool;
	}

	public Respose req(Cmd cmd, byte[]... vals) {
		try {
			SSDBStream steam = pool.borrowObject();
			try {
				return steam.req(cmd, vals);
			} finally {
				pool.returnObject(steam);
			}
		} catch (Exception e) {
			throw new SSDBException(e);
		}
	}

	public SSDBStream doClone() {
		return null; // 不支持克隆
	}

}
