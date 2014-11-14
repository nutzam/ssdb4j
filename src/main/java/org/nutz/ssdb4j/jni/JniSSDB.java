package org.nutz.ssdb4j.jni;

import java.io.IOException;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class JniSSDB implements SSDBStream {
	
	static {
		System.loadLibrary("ssdb");
		System.loadLibrary("ssdbjni");
	}
	
	public JniSSDB(String path) {
		int re = _init(path);
		if (re != 0) {
			throw new IllegalArgumentException("re="+re);
		}
	}

	public Response req(Cmd cmd, byte[]... vals) {
		byte[][] re = _req(cmd.bytes(), vals);
		Response resp = new Response();
		if (re == null || re.length == 0) {
			resp.stat = "error";
			return resp;
		} else {
			resp.stat = new String(re[0]);
			if (re.length > 1) {
				for (int i = 1; i < re.length; i++) {
					resp.datas.add(re[i]);
				}
			}
		}
		return resp;
	}

	public void callback(SSDBStreamCallback callback) {
		throw new RuntimeException("JNI Not impl callback");
	}

	public void close() throws IOException {
		int re = _close();
		if (re != 0) {
			throw new IOException("re="+re);
		}
	}

	protected native int _init(String cnf);
	protected native byte[][] _req(byte[] cmd, byte[]... vals);
	protected native int _close();
}
