package org.nutz.ssdb.spi;


public interface SSDBStream {
	
	Respose req(String cmd, byte[] ...vals);
	
	SSDBStream doClone();
}
