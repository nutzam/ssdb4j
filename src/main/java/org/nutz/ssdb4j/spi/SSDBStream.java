package org.nutz.ssdb4j.spi;

/**
 * 封装与服务器通信的逻辑
 * @author wendal(wendal1985@gmail.com)
 *
 */
public interface SSDBStream {
	
	Response req(Cmd cmd, byte[] ...vals);
	
	void callback(SSDBStreamCallback callback);
	
	/**关闭这个SSDBStream,对于池化的SSDBStream,这个方法通常是无操作的*/
	void close() throws Exception;
	
	/**销毁这个SSDBStream*/
	void depose() throws Exception;
}
