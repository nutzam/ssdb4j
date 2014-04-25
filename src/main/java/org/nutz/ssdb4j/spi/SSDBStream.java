package org.nutz.ssdb4j.spi;

/**
 * 封装与服务器通信的逻辑
 * @author wendal(wendal1985@gmail.com)
 *
 */
public interface SSDBStream {
	
	Respose req(Cmd cmd, byte[] ...vals);
	
	void callback(SSDBStreamCallback callback);
	
	void close() throws Exception;
}
