package org.nutz.ssdb4j.spi;

public interface ObjectConv {

	byte[] bytes(Object obj);
	
	byte[][] bytess(Object...objs);
}
