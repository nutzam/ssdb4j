package org.nutz.ssdb4j.impl;

import java.io.InputStream;
import java.io.OutputStream;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public abstract class AbstractIoSSDBStream implements SSDBStream {

	protected InputStream in;

	protected OutputStream out;

	public synchronized Response req(Cmd cmd, byte[]... vals) {
		beforeExec();
		try {
			SSDBs.sendCmd(out, cmd, vals);
			Response resp = SSDBs.readResp(in);
			beforeReturn(resp);
			return resp;
		} catch (Throwable e) {
			return whenError(e);
		}
	}

	protected void beforeExec() {
	}

	protected void beforeReturn(Response resp) {
	}

	protected Response whenError(Throwable e) {
		throw new SSDBException(e);
	}
	
	public void callback(SSDBStreamCallback callback) {
		beforeExec();
		try {
			callback.invoke(in, out);
		} catch (Throwable e) {
			whenError(e);
		}
	}
	
}
