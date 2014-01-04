package org.nutz.ssdb.spi;

/**
 * 封装与服务器通信的逻辑
 * @author wendal(wendal1985@gmail.com)
 *
 */
public interface SSDBStream {
	
	Respose req(Cmd cmd, byte[] ...vals);
	
	SSDBStream doClone();
}
