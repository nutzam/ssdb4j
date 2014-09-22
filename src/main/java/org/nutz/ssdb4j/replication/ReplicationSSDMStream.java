package org.nutz.ssdb4j.replication;

import java.io.IOException;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class ReplicationSSDMStream implements SSDBStream {

	protected SSDBStream master;
	
	protected SSDBStream slave;
	
	public ReplicationSSDMStream(SSDBStream master, SSDBStream slave) {
		this.master = master;
		this.slave = slave;
	}

	public Response req(Cmd cmd, byte[]... vals) {
		if (cmd.isSlave())
			return slave.req(cmd, vals);
		return master.req(cmd, vals);
	}

	public void callback(SSDBStreamCallback callback) {
		master.callback(callback);
	}
	
	public void close() throws IOException {
		try {
			master.close();
		} finally {
			slave.close();
		}
	}
}
