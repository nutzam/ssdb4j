package org.nutz.ssdb4j.spi;

import java.io.Closeable;

/**
 * 封装与服务器通信的逻辑
 * @author wendal(wendal1985@gmail.com)
 *
 */
public interface SSDBStream extends Closeable {
	
	Response req(Cmd cmd, byte[] ...vals);
	
	void callback(SSDBStreamCallback callback);
}
