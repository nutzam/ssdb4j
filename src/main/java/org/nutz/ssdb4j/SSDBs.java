package org.nutz.ssdb4j;

import org.nutz.ssdb.impl.SimpleClient;
import org.nutz.ssdb.spi.SSDB;

public class SSDBs {

	public static final SSDB make() {
		return new SimpleClient();
	}
	
	public static final SSDB make(String host, int port, int timeout) {
		return new SimpleClient(host, port, timeout);
	}
	
	public static String version() {
		return "7.5.2"; // 20130104 by wendal
	}
}
